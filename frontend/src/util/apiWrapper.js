import { Semaphore } from 'await-semaphore';
import axios from 'axios';
import { createDefaultBundleName, pathIsArchive, getArchiveExt } from './worksheet_utils';

export const get = (url, params, options) => {
    const requestOptions = {
        params,
        ...options,
    };
    return axios.get(url, requestOptions).then((res) => res.data);
};

export const post = (url, data, config) => {
    return axios.post(url, data, config).then((res) => res.data);
};

export const put = (url, data, config) => {
    return axios.put(url, data, config).then((res) => res.data);
};

export const patch = (url, data, config) => {
    return axios.patch(url, data, config).then((res) => res.data);
};

// prefixed with underscored because delete is a reserved word in javascript
export const _delete = (url, config) => {
    return axios.delete(url, config).then((res) => res.data);
};

export const defaultErrorHandler = (error) => {
    console.error(error);
};

export const updateEditableField = (url, data) => {
    return patch(url, data);
};

export const getUser = () => {
    return get('/rest/user');
};

export const getUsers = (username) => {
    const url = '/rest/users/' + username;
    return get(url);
};

export const updateUser = (data) => {
    const url = '/rest/user';
    return patch(url, { data });
};

export const navBarSearch = (keywords) => {
    const url = '/rest/interpret/wsearch';
    return post(url, { keywords });
};

export const addItems = (worksheetUUID, data) => {
    const url = `/rest/worksheets/${worksheetUUID}/add-items`;
    return post(url, data);
};

export const executeCommand = (command, worksheet_uuid) => {
    // returns a Promise
    const url = '/rest/cli/command';
    return post(url, {
        worksheet_uuid: worksheet_uuid || null,
        command: command,
    }).catch((error) => {
        const htmlDoc = new DOMParser().parseFromString(error.response.data, 'text/html');
        const exception = htmlDoc.getElementsByTagName('pre')[0].innerHTML;
        throw exception;
    });
};

export const completeCommand = (command, worksheet_uuid) => {
    // returns a Promise
    const url = '/rest/cli/command';
    return post(url, {
        worksheet_uuid: worksheet_uuid || null,
        command: command,
        autocomplete: true,
    });
};

// Limit concurrent requests for async resolving items
const MAX_CONCURRENT_REQUESTS = 3;
const semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);

export const fetchAsyncBundleContents = async ({ contents }) => {
    // used in table and record items
    return semaphore.use(async () => {
        const url = '/rest/interpret/genpath-table-contents';
        return await post(url, { contents });
    });
};

export const updateFileBrowser = (uuid, folder_path) => {
    const url = '/rest/bundles/' + uuid + '/contents/info/' + folder_path;
    return get(url, { depth: 1 });
};

export const fetchBundleContents = (uuid) => {
    const url = '/rest/bundles/' + uuid + '/contents/info/';
    return get(url, { depth: 1 });
};

export const fetchBundleStores = (uuid) => {
    const url = `/rest/bundles/${uuid}/locations/`;
    return get(url);
};

export const fetchBundleMetadata = (uuid) => {
    const url = '/rest/bundles/' + uuid;
    return get(url, {
        include_display_metadata: 1,
        include: 'owner,group_permissions,host_worksheets',
    });
};

export const fetchStores = (uuid) => {
    const url = '/rest/bundle_stores/' + uuid;
    return get(url);
};

export const fetchFileSummary = (uuid, path) => {
    const params = {
        head: 50,
        tail: 50,
        truncation_text: '\n... [truncated] ...\n\n',
    };
    const url =
        '/rest/bundles/' + uuid + '/contents/blob' + path + '?' + new URLSearchParams(params);
    return get(
        url,
        {
            headers: { Accept: 'text/plain' },
        },
        // need to define transformResponse due to the issue discussed in: https://github.com/axios/axios/issues/811
        { responseType: 'text', transformResponse: (data) => data },
    );
};

export async function createFileBundle(url, data, errorHandler) {
    try {
        return post(url, data);
    } catch (error) {
        errorHandler(error);
    }
}

export function getQueryParams(filename) {
    const formattedFilename = createDefaultBundleName(filename);
    const queryParams = {
        finalize_on_failure: 1,
        filename: pathIsArchive(filename)
            ? formattedFilename + getArchiveExt(filename)
            : formattedFilename,
        unpack: pathIsArchive(filename) ? 1 : 0,
    };
    return new URLSearchParams(queryParams);
}

// Upload the avatar image as a bundle to the bundle store
export const uploadImgAsync = (bundleUuid, file, fileName, errorHandler) => {
    return new Promise((resolve, reject) => {
        let reader = new FileReader();
        reader.onload = () => {
            let arrayBuffer = reader.result,
                bytesArray = new Uint8Array(arrayBuffer);
            let url = '/rest/bundles/' + bundleUuid + '/contents/blob/?' + getQueryParams(fileName);
            put(url, new Blob([bytesArray]))
                .then((data) => resolve(data))
                .catch((error) => {
                    errorHandler(error);
                    reject(error);
                });
        };
        reader.readAsArrayBuffer(file);
    });
};

export const fetchWorksheet = (uuid, queryParams) => {
    const url = '/rest/interpret/worksheet/' + uuid;
    return get(url, queryParams);
};

export const saveWorksheet = (uuid, data) => {
    const url = '/rest/worksheets/' + uuid + '/raw';
    return post(url, data);
};

export const deleteWorksheet = (uuid) => {
    const url = '/rest/worksheets?force=1';
    const data = { data: [{ id: uuid, type: 'worksheets' }] };
    return _delete(url, { data });
};

export const apiWrapper = {
    get,
    post,
    put,
    delete: _delete,
    defaultErrorHandler,
    patch,
    updateEditableField,
    getUser,
    updateUser,
    getUsers,
    navBarSearch,
    addItems,
    executeCommand,
    completeCommand,
    fetchAsyncBundleContents,
    updateFileBrowser,
    fetchBundleContents,
    fetchBundleMetadata,
    fetchFileSummary,
    createFileBundle,
    uploadImgAsync,
    getQueryParams,
    saveWorksheet,
    fetchWorksheet,
    deleteWorksheet,
};
