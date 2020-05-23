// Should match codalab/common.py#CODALAB_VERSION
export const CODALAB_VERSION = '0.5.13';

export const NAVBAR_HEIGHT = 60;

// Worksheet width
export const EXPANDED_WORKSHEET_WIDTH = '99%';
export const DEFAULT_WORKSHEET_WIDTH = '65%';
export const FILE_SIZE_LIMIT_GB = 2;
export const FILE_SIZE_LIMIT_B = FILE_SIZE_LIMIT_GB * 1024 * 1024 * 1024;
export const LOCAL_STORAGE_WORKSHEET_WIDTH = 'worksheetWidth';

// Bundle fetch status values; corresponds with FetchStatusCodes in backend
export const FETCH_STATUS_SCHEMA = {
    UNKNOWN: 'unknown',
    PENDING: 'pending',
    BRIEFLY_LOADED: 'briefly_loaded',
    READY: 'ready',
    NOT_FOUND: 'not_found',
    NO_PERMISSION: 'no_permission',
};
