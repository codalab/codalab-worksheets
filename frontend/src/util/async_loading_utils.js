import { Semaphore } from 'await-semaphore';
import $ from 'jquery';

// Limit concurrent requests for async resolving items
const MAX_CONCURRENT_REQUESTS = 3;
export const semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);

export async function fetchAsyncBundleContents({ contents }) {
    // used in table and record items
    return semaphore.use(async () => {
        const response = await $.ajax({
            type: 'POST',
            contentType: 'application/json',
            url: '/rest/interpret/genpath-table-contents',
            async: true,
            data: JSON.stringify({ contents }),
            dataType: 'json',
            cache: false,
        });
        return response;
    });
}
