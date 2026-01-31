import { BN, Event, Program, Provider } from "@coral-xyz/anchor";
import { unpackAccount, unpackMint } from "@solana/spl-token";
import { AccountInfo, Connection, PublicKey } from "@solana/web3.js";
import Decimal from "decimal.js";
import { InstructionParser } from "./lib/instruction-parser";
import { DecimalUtil } from "./lib/utils";
import { getEvents } from "./lib/get-events";
import { AMM_TYPES, JUPITER_V6_PROGRAM_ID } from "./constants";
import { FeeEvent, SwapEvent, TransactionWithMeta } from "./types";
import { IDL, Jupiter } from "./idl/jupiter";

export { TransactionWithMeta };

export const program = new Program<Jupiter>(
  IDL,
  JUPITER_V6_PROGRAM_ID,
  {} as Provider
);

type AccountInfoMap = Map<string, AccountInfo<Buffer>>;

export type SwapAttributes = {
  owner: string;
  transferAuthority: string;
  programId: string;
  signature: string;
  timestamp: Date;
  legCount: number;
  inSymbol: string;
  inAmount: BigInt;
  inAmountInDecimal?: number;
  inMint: string;
  outSymbol: string;
  outAmount: BigInt;
  outAmountInDecimal?: number;
  outMint: string;
  instruction: string;
  exactInAmount: BigInt;
  exactOutAmount: BigInt;
  swapData: JSON;
  feeTokenPubkey?: string;
  feeOwner?: string;
  feeSymbol?: string;
  feeAmount?: BigInt;
  feeAmountInDecimal?: number;
  feeMint?: string;
  tokenLedger?: string;
  lastAccount: string; // This can be a tracking account since we don't have a way to know we just log it the last account.
};

const reduceEventData = <T>(events: Event[], name: string) =>
  events.reduce((acc, event) => {
    if (event.name === name) {
      acc.push(event.data as T);
    }
    return acc;
  }, new Array<T>());

export async function extract(
  signature: string,
  connection: Connection | null,
  tx: TransactionWithMeta,
  blockTime?: number
): Promise<SwapAttributes | undefined> {
  console.log("[extract] Starting extraction for signature:", signature);
  
  const programId = JUPITER_V6_PROGRAM_ID;
  const accountInfosMap: AccountInfoMap = new Map();

  const logMessages = tx.meta.logMessages;
  console.log("[extract] Log messages count:", logMessages?.length ?? 0);
  if (!logMessages) {
    console.log("[extract] ERROR: Missing log messages");
    throw new Error("Missing log messages...");
  }

  const parser = new InstructionParser(programId);
  console.log("[extract] Getting events from transaction...");
  const events = getEvents(program, tx);
  console.log("[extract] Total events found:", events.length);

  const swapEvents = reduceEventData<SwapEvent>(events, "SwapEvent");
  const feeEvent = reduceEventData<FeeEvent>(events, "FeeEvent")[0];
  console.log("[extract] SwapEvents count:", swapEvents.length, "FeeEvent:", !!feeEvent);

  if (swapEvents.length === 0) {
    // Not a swap event, for example: https://solscan.io/tx/5ZSozCHmAFmANaqyjRj614zxQY8HDXKyfAs2aAVjZaadS4DbDwVq8cTbxmM5m5VzDcfhysTSqZgKGV1j2A2Hqz1V
    console.log("[extract] No swap events found, returning undefined");
    return;
  }

  // Only fetch account info if connection is provided (for decimal calculations)
  if (connection) {
    const accountsToBeFetched = new Array<PublicKey>();
    swapEvents.forEach((swapEvent) => {
      accountsToBeFetched.push(swapEvent.inputMint);
      accountsToBeFetched.push(swapEvent.outputMint);
    });

    if (feeEvent) {
      accountsToBeFetched.push(feeEvent.account);
    }
    const accountInfos = await connection.getMultipleAccountsInfo(
      accountsToBeFetched
    );
    accountsToBeFetched.forEach((account, index) => {
      accountInfosMap.set(account.toBase58(), accountInfos[index]);
    });
  }
  console.log("[extract] Account info map size:", accountInfosMap.size, "(0 means no RPC call made)");

  console.log("[extract] Parsing swap events...");
  const swapData = await parseSwapEvents(accountInfosMap, swapEvents);
  console.log("[extract] SwapData parsed:", swapData.length, "items");
  
  const instructions = parser.getInstructions(tx);
  console.log("[extract] Instructions found:", instructions.length);
  
  const positions = parser.getInitialAndFinalSwapPositions(instructions);
  console.log("[extract] Positions result:", positions);
  
  // Handle case where positions can't be determined (e.g., unknown swap types, single-hop swaps)
  // Also check if the position arrays are empty - this happens for single-hop swaps
  if (!positions || positions.length !== 2 || positions[0].length === 0 || positions[1].length === 0) {
    console.log("[extract] Using fallback positions (first/last swap events)");
    // Fallback: use first and last swap events
    const initialPositions = [0];
    const finalPositions = [swapData.length - 1];
    
    const inSymbol = null;
    const inMint = swapData[initialPositions[0]].inMint;
    const inSwapData = swapData.filter(
      (swap, index) => initialPositions.includes(index) && swap.inMint === inMint
    );
    const inAmount = inSwapData.reduce((acc, curr) => {
      return acc + BigInt(curr.inAmount);
    }, BigInt(0));
    const inAmountInDecimal = inSwapData.reduce((acc, curr) => {
      return acc.add(curr.inAmountInDecimal ?? 0);
    }, new Decimal(0));

    const outSymbol = null;
    const outMint = swapData[finalPositions[0]].outMint;
    const outSwapData = swapData.filter(
      (swap, index) => finalPositions.includes(index) && swap.outMint === outMint
    );
    const outAmount = outSwapData.reduce((acc, curr) => {
      return acc + BigInt(curr.outAmount);
    }, BigInt(0));
    const outAmountInDecimal = outSwapData.reduce((acc, curr) => {
      return acc.add(curr.outAmountInDecimal ?? 0);
    }, new Decimal(0));

    const swap = {} as SwapAttributes;

    const instructionInfo = parser.getInstructionNameAndTransferAuthorityAndLastAccount(instructions);
    let [instructionName, transferAuthority, lastAccount] = instructionInfo.length === 3 
      ? instructionInfo 
      : ['unknown', '', ''];

    // Fallback: if instruction decoding failed, try to extract transferAuthority from instruction accounts
    // For Jupiter, transferAuthority is typically at index 1 (route) or index 2 (sharedAccountsRoute)
    if (!transferAuthority && instructions.length > 0) {
      const jupiterInstruction = instructions[0];
      if (jupiterInstruction.accounts.length > 2) {
        // Try index 2 first (sharedAccountsRoute is most common)
        transferAuthority = jupiterInstruction.accounts[2].toString();
        lastAccount = jupiterInstruction.accounts[jupiterInstruction.accounts.length - 1].toString();
        console.log("[extract] Fallback: extracted transferAuthority from instruction accounts[2]:", transferAuthority);
      } else if (jupiterInstruction.accounts.length > 1) {
        // Fall back to index 1
        transferAuthority = jupiterInstruction.accounts[1].toString();
        lastAccount = jupiterInstruction.accounts[jupiterInstruction.accounts.length - 1].toString();
        console.log("[extract] Fallback: extracted transferAuthority from instruction accounts[1]:", transferAuthority);
      }
    }

    swap.transferAuthority = transferAuthority;
    swap.lastAccount = lastAccount;
    swap.instruction = instructionName;
    swap.owner = tx.transaction.message.accountKeys[0].pubkey.toBase58();
    swap.programId = programId.toBase58();
    swap.signature = signature;
    swap.timestamp = new Date(new Date((blockTime ?? 0) * 1000).toISOString());
    swap.legCount = swapEvents.length;

    swap.inSymbol = inSymbol;
    swap.inAmount = inAmount;
    swap.inAmountInDecimal = inAmountInDecimal.toNumber();
    swap.inMint = inMint;

    swap.outSymbol = outSymbol;
    swap.outAmount = outAmount;
    swap.outAmountInDecimal = outAmountInDecimal.toNumber();
    swap.outMint = outMint;

    swap.swapData = JSON.parse(JSON.stringify(swapData));

    if (feeEvent) {
      const { mint, amount, amountInDecimal } = await extractVolume(
        accountInfosMap,
        feeEvent.mint,
        feeEvent.amount
      );
      swap.feeTokenPubkey = feeEvent.account.toBase58();
      swap.feeOwner = extractTokenAccountOwner(
        accountInfosMap,
        feeEvent.account
      )?.toBase58();
      swap.feeAmount = BigInt(amount);
      swap.feeAmountInDecimal = amountInDecimal?.toNumber();
      swap.feeMint = mint;
    }

    console.log("[extract] Returning swap (fallback path):", { inMint: swap.inMint, outMint: swap.outMint, inAmount: swap.inAmount?.toString(), outAmount: swap.outAmount?.toString() });
    return swap;
  }

  const [initialPositions, finalPositions] = positions;
  console.log("[extract] Using normal path with positions:", { initialPositions, finalPositions });

  const inSymbol = null; // We don't longer support this.
  const inMint = swapData[initialPositions[0]].inMint;
  const inSwapData = swapData.filter(
    (swap, index) => initialPositions.includes(index) && swap.inMint === inMint
  );
  const inAmount = inSwapData.reduce((acc, curr) => {
    return acc + BigInt(curr.inAmount);
  }, BigInt(0));
  const inAmountInDecimal = inSwapData.reduce((acc, curr) => {
    return acc.add(curr.inAmountInDecimal ?? 0);
  }, new Decimal(0));

  const outSymbol = null; // We don't longer support this.
  const outMint = swapData[finalPositions[0]].outMint;
  const outSwapData = swapData.filter(
    (swap, index) => finalPositions.includes(index) && swap.outMint === outMint
  );
  const outAmount = outSwapData.reduce((acc, curr) => {
    return acc + BigInt(curr.outAmount);
  }, BigInt(0));
  const outAmountInDecimal = outSwapData.reduce((acc, curr) => {
    return acc.add(curr.outAmountInDecimal ?? 0);
  }, new Decimal(0));

  const swap = {} as SwapAttributes;

  const [instructionName, transferAuthority, lastAccount] =
    parser.getInstructionNameAndTransferAuthorityAndLastAccount(instructions);

  swap.transferAuthority = transferAuthority;
  swap.lastAccount = lastAccount;
  swap.instruction = instructionName;
  swap.owner = tx.transaction.message.accountKeys[0].pubkey.toBase58();
  swap.programId = programId.toBase58();
  swap.signature = signature;
  swap.timestamp = new Date(new Date((blockTime ?? 0) * 1000).toISOString());
  swap.legCount = swapEvents.length;

  swap.inSymbol = inSymbol;
  swap.inAmount = inAmount;
  swap.inAmountInDecimal = inAmountInDecimal.toNumber();
  swap.inMint = inMint;

  swap.outSymbol = outSymbol;
  swap.outAmount = outAmount;
  swap.outAmountInDecimal = outAmountInDecimal.toNumber();
  swap.outMint = outMint;

  const exactOutAmount = parser.getExactOutAmount(
    tx.transaction.message.instructions
  );
  if (exactOutAmount) {
    swap.exactOutAmount = BigInt(exactOutAmount);
  }

  const exactInAmount = parser.getExactInAmount(
    tx.transaction.message.instructions
  );
  if (exactInAmount) {
    swap.exactInAmount = BigInt(exactInAmount);
  }

  swap.swapData = JSON.parse(JSON.stringify(swapData));

  if (feeEvent) {
    const { mint, amount, amountInDecimal } = await extractVolume(
      accountInfosMap,
      feeEvent.mint,
      feeEvent.amount
    );
    swap.feeTokenPubkey = feeEvent.account.toBase58();
    swap.feeOwner = extractTokenAccountOwner(
      accountInfosMap,
      feeEvent.account
    )?.toBase58();
    swap.feeAmount = BigInt(amount);
    swap.feeAmountInDecimal = amountInDecimal?.toNumber();
    swap.feeMint = mint;
  }

  console.log("[extract] Returning swap (normal path):", { inMint: swap.inMint, outMint: swap.outMint, inAmount: swap.inAmount?.toString(), outAmount: swap.outAmount?.toString() });
  return swap;
}

async function parseSwapEvents(
  accountInfosMap: AccountInfoMap,
  swapEvents: SwapEvent[]
) {
  const swapData = await Promise.all(
    swapEvents.map((swapEvent) => extractSwapData(accountInfosMap, swapEvent))
  );

  return swapData;
}

async function extractSwapData(
  accountInfosMap: AccountInfoMap,
  swapEvent: SwapEvent
) {
  const amm =
    AMM_TYPES[swapEvent.amm.toBase58()] ??
    `Unknown program ${swapEvent.amm.toBase58()}`;

  const {
    mint: inMint,
    amount: inAmount,
    amountInDecimal: inAmountInDecimal,
  } = await extractVolume(
    accountInfosMap,
    swapEvent.inputMint,
    swapEvent.inputAmount
  );
  const {
    mint: outMint,
    amount: outAmount,
    amountInDecimal: outAmountInDecimal,
  } = await extractVolume(
    accountInfosMap,
    swapEvent.outputMint,
    swapEvent.outputAmount
  );

  return {
    amm,
    inMint,
    inAmount,
    inAmountInDecimal,
    outMint,
    outAmount,
    outAmountInDecimal,
  };
}

async function extractVolume(
  accountInfosMap: AccountInfoMap,
  mint: PublicKey,
  amount: BN
) {
  const tokenDecimals = extractMintDecimals(accountInfosMap, mint);
  const amountInDecimal = DecimalUtil.fromBN(amount, tokenDecimals);
  return {
    mint: mint.toBase58(),
    amount: amount.toString(),
    amountInDecimal,
  };
}

function extractTokenAccountOwner(
  accountInfosMap: AccountInfoMap,
  account: PublicKey
) {
  const accountData = accountInfosMap.get(account.toBase58());

  if (accountData) {
    const accountInfo = unpackAccount(account, accountData, accountData.owner);
    return accountInfo.owner;
  }

  return;
}

function extractMintDecimals(accountInfosMap: AccountInfoMap, mint: PublicKey) {
  const mintData = accountInfosMap.get(mint.toBase58());

  if (mintData) {
    const mintInfo = unpackMint(mint, mintData, mintData.owner);
    return mintInfo.decimals;
  }

  return;
}
