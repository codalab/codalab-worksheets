import { Semaphore } from 'await-semaphore';

// Limit concurrent requests for async resolving items
const MAX_CONCURRENT_REQUESTS = 3;
export const semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);
