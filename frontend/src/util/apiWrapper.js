const get = (url, ajaxOptions) => {
    const requestOptions = {
        method: 'GET',
    };
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

const updateEditableField = (url, data, callback) => {
    patch(url, data)
        .then(callback)
        .catch(defaultErrorHandler);
};

const getUser = (callback) => {
    const url = '/rest/user';
    get(url)
        .then(callback)
        .catch(defaultErrorHandler);
};

const getUsers = (username, callback) => {
    const url = '/rest/users/' + username;
    get(url)
        .then(callback)
        .catch(defaultErrorHandler);
};

const updateUser = (newUser, callback) => {
    const url = '/rest/user';
    patch(url, { data: newUser })
        .then(callback)
        .catch(defaultErrorHandler);
};

const navBarSearch = (keywords, callback) => {
    const url = '/rest/interpret/wsearch';
    post(url, { keywords })
        .then(callback)
        .catch(defaultErrorHandler);
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
};
