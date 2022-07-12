const uploadBundleStates = ['created', 'uploading', 'ready | failed'];
const makeBundleStates = ['created', 'making', 'ready | failed'];
const runBundleStates = [
    'created',
    'staged',
    'starting',
    'preparing',
    'running',
    'finalizing',
    'ready | failed | killed',
];
const allBundleStates = [
    'created',
    'uploading | making | staged',
    'starting [run bundles only]',
    'preparing [run bundles only]',
    'running [run bundles only]',
    'finalizing [run bundles only]',
    'ready | failed | killed',
];

/**
 * Return an array of bundle states based on bundle type.
 * All possible final states of a bundle are grouped together (e.g. 'ready | failed').
 *
 * @param {string} bundleType
 * @returns {array}
 */
export function getBundleStates(bundleType) {
    if (bundleType === 'dataset') {
        return uploadBundleStates;
    }
    if (bundleType === 'make') {
        return makeBundleStates;
    }
    if (bundleType === 'run') {
        return runBundleStates;
    }
    return allBundleStates; // show all states when bundle type is not given
}
