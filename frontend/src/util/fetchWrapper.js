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

export const fetchWrapper = {
    get,
    post,
    put,
    delete: _delete,
};
