import type { ExampleState } from './reducers';

/**
 * Get example data.
 * @param state
 * @returns Data stored in state.
 */
export function getExampleData(state: ExampleState): number {
    return state.data;
}
