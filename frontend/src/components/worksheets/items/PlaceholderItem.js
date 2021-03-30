import React, { useEffect, useState, forwardRef } from 'react';
import queryString from 'query-string';
import './PlaceholderItem.scss';
import useSWR, { cache } from 'swr';

const fetcher = (url) =>
    fetch(url, {
        type: 'GET',
        async: true,
        dataType: 'json',
    }).then((r) => {
        return r.json();
    });

export default forwardRef((props, ref) => {
    const [item, setItem] = useState(undefined);
    const [error, setError] = useState(false);
    const { worksheetUUID, onAsyncItemLoad, itemHeight } = props;
    const { directive, sort_keys } = props.item;
    function setBlocks(data) {
        const blocks = data.blocks;
        try {
            setItem(blocks.length === 0 ? null : blocks[0]);
            if (blocks.length > 0) {
                let actualBlock = blocks[0];
                // replace with existing sort keys if there is one
                if (sort_keys) {
                    actualBlock['sort_keys'] = sort_keys;
                }
                actualBlock.loadedFromPlaceholder = true;
                onAsyncItemLoad(actualBlock);
            }
        } catch (e) {
            console.error(e);
            setError(e);
        }
    }
    const url =
        '/rest/interpret/worksheet/' + worksheetUUID + '?' + queryString.stringify({ directive });
    // use data stored in cache

    useEffect(() => {
        if (cache.has(url)) {
            setBlocks(cache.get(url));
        }
    }, []);

    // fetch data only once
    useSWR(url, fetcher, {
        revalidateOnMount: !cache.has(url),
        onSuccess: (data, key, config) => {
            setBlocks(data);
        },
    });

    if (error) {
        return <div ref={ref}>Error loading item.</div>;
    }
    if (item === null) {
        // No items
        return <div ref={ref}>No results found.</div>;
    }
    return (
        <div ref={ref} className='codalab-item-placeholder' style={{ height: itemHeight || 100 }} />
    );
});
