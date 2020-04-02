import React, { useState, useEffect, forwardRef } from 'react';
import $ from 'jquery';
import queryString from 'query-string';
import './PlaceholderItem.scss';
import { BLOCK_TO_COMPONENT } from '../WorksheetItemList';
import { Semaphore } from 'await-semaphore';

// Limit concurrent requests for resolving placeholder items
const MAX_CONCURRENT_REQUESTS = 3;
const semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);

async function fetchData({ worksheetUUID, directive }) {
    return semaphore.use(async () => {
        const queryParams = {
            directive,
        };
        const info = await $.ajax({
            type: 'GET',
            url:
                '/rest/interpret/worksheet/' +
                worksheetUUID +
                '?' +
                queryString.stringify(queryParams),
            async: true,
            dataType: 'json',
            cache: false,
        });
        return info;
    });
}

export default forwardRef((props, ref) => {
    const [item, setItem] = useState(null);
    const [error, setError] = useState(false);
    const { worksheetUUID } = props;
    const { directive } = props.item;
    useEffect(() => {
        (async function() {
            try {
                const { items } = await fetchData({ directive, worksheetUUID });
                setItem(items.length === 0 ? null : items[0]);
            } catch (e) {
                console.error(e);
                setError(e);
            }
        })();
    }, [directive, worksheetUUID]);
    if (error) {
        return <div ref={ref}>Error loading item.</div>;
    }
    if (item === null) {
        return null;
    }
    if (!item) {
        return <div ref={ref} className='codalab-item-placeholder'></div>;
    }
    const Comp = BLOCK_TO_COMPONENT[item.mode];
    return <Comp {...props} item={item} ref={ref} />;
});
