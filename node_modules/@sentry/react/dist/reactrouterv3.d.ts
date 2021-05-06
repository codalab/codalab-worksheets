import { Location, ReactRouterInstrumentation } from './types';
declare type HistoryV3 = {
    location?: Location;
    listen?(cb: (location: Location) => void): void;
} & Record<string, any>;
export declare type Route = {
    path?: string;
    childRoutes?: Route[];
};
export declare type Match = (props: {
    location: Location;
    routes: Route[];
}, cb: (error?: Error, _redirectLocation?: Location, renderProps?: {
    routes?: Route[];
}) => void) => void;
/**
 * Creates routing instrumentation for React Router v3
 * Works for React Router >= 3.2.0 and < 4.0.0
 *
 * @param history object from the `history` library
 * @param routes a list of all routes, should be
 * @param match `Router.match` utility
 */
export declare function reactRouterV3Instrumentation(history: HistoryV3, routes: Route[], match: Match): ReactRouterInstrumentation;
export {};
//# sourceMappingURL=reactrouterv3.d.ts.map