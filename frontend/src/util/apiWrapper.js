import { Semaphore } from 'await-semaphore';

const get = (url, params, ajaxOptions) => {
    const requestOptions = {
        method: 'GET',
    };
    if (params !== undefined) {
        url = url + '?' + new URLSearchParams(params);
    }
    return fetch(url, requestOptions).then(handleResponse);
};

const post = (url, body, ajaxOptions) => {
    const requestOptions = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    };
    return fetch(url, requestOptions).then(handleResponse);
};

const put = (url, body, ajaxOptions) => {
    const requestOptions = {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    };
    return fetch(url, requestOptions).then(handleResponse);
};

const patch = (url, body, ajaxOptions) => {
    const requestOptions = {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    };
    return fetch(url, requestOptions).then(handleResponse);
};

// prefixed with underscored because delete is a reserved word in javascript
const _delete = (url) => {
    const requestOptions = {
        method: 'DELETE',
    };
    return fetch(url, requestOptions).then(handleResponse);
};

// helper functions
const handleResponse = (response) => {
    return response.text().then((text) => {
        const data = text && JSON.parse(text);

        if (!response.ok) {
            const error = (data && data.message) || response.statusText;
            return Promise.reject(error);
        }
        return data;
    });
};

const defaultErrorHandler = (error) => {
    console.error(error);
};

const updateEditableField = (url, data, callback, errorHandler = defaultErrorHandler) => {
    patch(url, data)
        .then(callback)
        .catch(errorHandler);
};

const getUser = (callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/user';
    get(url)
        .then(callback)
        .catch(errorHandler);
};

const getUsers = (username, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/users/' + username;
    get(url)
        .then(callback)
        .catch(errorHandler);
};

const updateUser = (newUser, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/user';
    patch(url, { data: newUser })
        .then(callback)
        .catch(errorHandler);
};

const navBarSearch = (keywords, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/interpret/wsearch';
    post(url, { keywords })
        .then(callback)
        .catch(errorHandler);
};

const addItems = (worksheetUUID, data, callback, errorHandler = defaultErrorHandler) => {
    const url = `/rest/worksheets/${worksheetUUID}/add-items`;
    post(url, data)
        .then(callback)
        .catch(errorHandler);
};

const executeCommand = (command, worksheet_uuid) => {
    // returns a Promise
    const url = '/rest/cli/command';
    return post(url, {
        worksheet_uuid: worksheet_uuid || null,
        command: command,
    });
};

const completeCommand = (command, worksheet_uuid) => {
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

const fetchAsyncBundleContents = async ({ contents }) => {
    // used in table and record items
    return semaphore.use(async () => {
        const url = '/rest/interpret/genpath-table-contents';
        return await post(url, { contents });
    });
};

const updateFileBrowser = (uuid, folder_path, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/bundles/' + uuid + '/contents/info/' + folder_path;
    get(url, { depth: 1 })
        .then(callback)
        .catch(errorHandler);
};

const fetchBundleContents = (uuid, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/bundles/' + uuid + '/contents/info/';
    get(url, { depth: 1 })
        .then(callback)
        .catch(errorHandler);
};

const fetchBundleMetadata = (uuid, callback, errorHandler = defaultErrorHandler) => {
    const url = '/rest/bundles/' + uuid;
    get(url, {
        include_display_metadata: 1,
        include: 'owner,group_permissions,host_worksheets',
    })
        .then(callback)
        .catch(errorHandler);
};

const fetchFileSummary = (uuid, path) => {
    const params = {
        head: 50,
        tail: 50,
        truncation_text: '\n... [truncated] ...\n\n',
    };
    const url =
        '/rest/bundles/' + uuid + '/contents/blob' + path + '?' + new URLSearchParams(params);
    return fetch(url, {
        method: 'GET',
        headers: {
            Accept: 'text/plain',
        },
    }).then((res) => res.text());
};

export const apiWrapper = {
    get,
    post,
    put,
    delete: _delete,
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
};
