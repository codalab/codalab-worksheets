import { Hub } from '@sentry/browser';
import { Transaction } from '@sentry/types';
import * as React from 'react';
export declare const UNKNOWN_COMPONENT = "unknown";
export declare type ProfilerProps = {
    name: string;
    disabled?: boolean;
    includeRender?: boolean;
    includeUpdates?: boolean;
    updateProps: {
        [key: string]: unknown;
    };
};
/**
 * The Profiler component leverages Sentry's Tracing integration to generate
 * spans based on component lifecycles.
 */
declare class Profiler extends React.Component<ProfilerProps> {
    private _mountActivity;
    private _mountSpan;
    static defaultProps: Partial<ProfilerProps>;
    constructor(props: ProfilerProps);
    componentDidMount(): void;
    componentDidUpdate({ updateProps, includeUpdates }: ProfilerProps): void;
    componentWillUnmount(): void;
    render(): React.ReactNode;
}
/**
 * withProfiler is a higher order component that wraps a
 * component in a {@link Profiler} component. It is recommended that
 * the higher order component be used over the regular {@link Profiler} component.
 *
 * @param WrappedComponent component that is wrapped by Profiler
 * @param options the {@link ProfilerProps} you can pass into the Profiler
 */
declare function withProfiler<P extends Record<string, any>>(WrappedComponent: React.ComponentType<P>, options?: Pick<Partial<ProfilerProps>, Exclude<keyof ProfilerProps, 'updateProps'>>): React.FC<P>;
/**
 *
 * `useProfiler` is a React hook that profiles a React component.
 *
 * Requires React 16.8 or above.
 * @param name displayName of component being profiled
 */
declare function useProfiler(name: string, options?: {
    disabled?: boolean;
    hasRenderSpan?: boolean;
}): void;
export { withProfiler, Profiler, useProfiler };
/** Grabs active transaction off scope */
export declare function getActiveTransaction<T extends Transaction>(hub?: Hub): T | undefined;
//# sourceMappingURL=profiler.d.ts.map