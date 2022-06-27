export const bundleStates = [
    'created',
    'uploading',
    'staged',
    'starting',
    'preparing',
    'running',
    'finalizing',
    'ready, failed or killed',
];

// everything in bundleStates + a `making` state
export const makeBundleStates = [
    'created',
    'uploading',
    'staged',
    'making',
    'starting',
    'preparing',
    'running',
    'finalizing',
    'ready, failed or killed',
];

export const offlineState = 'worker_offline';
