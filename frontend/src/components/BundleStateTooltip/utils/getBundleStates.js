const uploadBundleStates = ['created', 'uploading', 'ready or failed'];

const makeBundleStates = ['created', 'making', 'ready or failed'];

const runBundleStates = [
    'created',
    'staged',
    'starting',
    'preparing',
    'running',
    'finalizing',
    'ready, failed or killed',
    'worker_offline',
];

const allBundleStates = [
    'created',
    'uploading, making or staged',
    'starting [run bundles only]',
    'preparing [run bundles only]',
    'running [run bundles only]',
    'finalizing [run bundles only]',
    'ready, failed or killed',
    'worker_offline',
];

/**
 * Return an array of bundle states based on bundle type.
 * All possible final states of a bundle are grouped together (e.g. 'ready or failed').
 *
 * @param {string} bundleType
 * @returns {array}
 */
export function getBundleStates(bundleType) {
    if (bundleType == 'dataset') {
        return uploadBundleStates;
    }
    if (bundleType == 'make') {
        return makeBundleStates;
    }
    if (bundleType == 'run') {
        return runBundleStates;
    }
    return allBundleStates; // show all states when bundle type is not given
}
