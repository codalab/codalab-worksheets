import { ReportDialogOptions, Scope } from '@sentry/browser';
import * as React from 'react';
export declare const UNKNOWN_COMPONENT = "unknown";
export declare type FallbackRender = (errorData: {
    error: Error;
    componentStack: string | null;
    eventId: string | null;
    resetError(): void;
}) => React.ReactNode;
export declare type ErrorBoundaryProps = {
    /** If a Sentry report dialog should be rendered on error */
    showDialog?: boolean;
    /**
     * Options to be passed into the Sentry report dialog.
     * No-op if {@link showDialog} is false.
     */
    dialogOptions?: ReportDialogOptions;
    /**
     * A fallback component that gets rendered when the error boundary encounters an error.
     *
     * Can either provide a React Component, or a function that returns React Component as
     * a valid fallback prop. If a function is provided, the function will be called with
     * the error, the component stack, and an function that resets the error boundary on error.
     *
     */
    fallback?: React.ReactNode | FallbackRender;
    /** Called with the error boundary encounters an error */
    onError?(error: Error, componentStack: string, eventId: string): void;
    /** Called on componentDidMount() */
    onMount?(): void;
    /** Called if resetError() is called from the fallback render props function  */
    onReset?(error: Error | null, componentStack: string | null, eventId: string | null): void;
    /** Called on componentWillUnmount() */
    onUnmount?(error: Error | null, componentStack: string | null, eventId: string | null): void;
    /** Called before the error is captured by Sentry, allows for you to add tags or context using the scope */
    beforeCapture?(scope: Scope, error: Error | null, componentStack: string | null): void;
};
declare type ErrorBoundaryState = {
    componentStack: string | null;
    error: Error | null;
    eventId: string | null;
};
/**
 * A ErrorBoundary component that logs errors to Sentry.
 * Requires React >= 16
 */
declare class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
    state: ErrorBoundaryState;
    componentDidCatch(error: Error, { componentStack }: React.ErrorInfo): void;
    componentDidMount(): void;
    componentWillUnmount(): void;
    resetErrorBoundary: () => void;
    render(): React.ReactNode;
}
declare function withErrorBoundary<P extends Record<string, any>>(WrappedComponent: React.ComponentType<P>, errorBoundaryOptions: ErrorBoundaryProps): React.FC<P>;
export { ErrorBoundary, withErrorBoundary };
//# sourceMappingURL=errorboundary.d.ts.map