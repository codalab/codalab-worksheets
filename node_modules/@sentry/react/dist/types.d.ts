import { Transaction, TransactionContext } from '@sentry/types';
export declare type Action = 'PUSH' | 'REPLACE' | 'POP';
export declare type Location = {
    pathname: string;
    action?: Action;
} & Record<string, any>;
export declare type ReactRouterInstrumentation = <T extends Transaction>(startTransaction: (context: TransactionContext) => T | undefined, startTransactionOnPageLoad?: boolean, startTransactionOnLocationChange?: boolean) => void;
//# sourceMappingURL=types.d.ts.map