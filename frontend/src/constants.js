// Should match codalab/common.py#CODALAB_VERSION
export const CODALAB_VERSION = '1.0.3';

// Name Regex to match the backend in spec_utils.py
export const NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_.-]*$/i;

export const NAVBAR_HEIGHT = 60;
export const HEADER_HEIGHT = 170;

// Worksheet width
export const EXPANDED_WORKSHEET_WIDTH = '99%';
export const NARROW_WORKSHEET_WIDTH = '65%';
export const FILE_SIZE_LIMIT_GB = 2;
export const FILE_SIZE_LIMIT_B = FILE_SIZE_LIMIT_GB * 1024 * 1024 * 1024;
export const LOCAL_STORAGE_WORKSHEET_WIDTH = 'newWorksheetWidth';

// Dialog constants
export const DIALOG_TYPES = {
    OPEN_DELETE_BUNDLE: 'delete_bundle',
    OPEN_KILL: 'kill_bundle',
    OPEN_DELETE_MARKDOWN: 'delete_markdown_block',
    OPEN_DELETE_WORKSHEET: 'delete_whole_worksheet',
    OPEN_ERROR_DIALOG: 'error_dialog',
    OPEN_DELETE_SCHEMA: 'delete_schema',
    OPEN_CREATE_CONTENT: 'create_content_block',
};

// Bundle fetch status values; corresponds with FetchStatusCodes in backend
export const FETCH_STATUS_SCHEMA = {
    UNKNOWN: 'unknown',
    PENDING: 'pending',
    BRIEFLY_LOADED: 'briefly_loaded',
    READY: 'ready',
    NOT_FOUND: 'not_found',
    NO_PERMISSION: 'no_permission',
};

// Default Duration for messagePopover shown on the worksheet
export const AUTO_HIDDEN_DURATION = 1500;

// All possible bundle states
export const BUNDLE_STATES: String[] = [
    'uploading',
    'created',
    'staged',
    'making',
    'starting',
    'preparing',
    'running',
    'finalizing',
    'ready',
    'failed',
    'killed',
    'worker_offline',
];
