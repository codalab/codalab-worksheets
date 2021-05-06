import * as React from 'react';
import { Action, Location, ReactRouterInstrumentation } from './types';
declare type Match = {
    path: string;
    url: string;
    params: Record<string, any>;
    isExact: boolean;
};
export declare type RouterHistory = {
    location?: Location;
    listen?(cb: (location: Location, action: Action) => void): void;
} & Record<string, any>;
export declare type RouteConfig = {
    [propName: string]: any;
    path?: string | string[];
    exact?: boolean;
    component?: JSX.Element;
    routes?: RouteConfig[];
};
interface RouteProps {
    [propName: string]: any;
    location?: Location;
    component?: React.ComponentType<any> | React.ComponentType<any>;
    render?: (props: any) => React.ReactNode;
    children?: ((props: any) => React.ReactNode) | React.ReactNode;
    path?: string | string[];
    exact?: boolean;
    sensitive?: boolean;
    strict?: boolean;
}
declare type MatchPath = (pathname: string, props: string | string[] | any, parent?: Match | null) => Match | null;
export declare function reactRouterV4Instrumentation(history: RouterHistory, routes?: RouteConfig[], matchPath?: MatchPath): ReactRouterInstrumentation;
export declare function reactRouterV5Instrumentation(history: RouterHistory, routes?: RouteConfig[], matchPath?: MatchPath): ReactRouterInstrumentation;
export declare function withSentryRouting<P extends RouteProps & Record<string, any>>(Route: React.ComponentType<P>): React.FC<P>;
export {};
//# sourceMappingURL=reactrouter.d.ts.map