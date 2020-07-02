import React, { useState, useEffect, forwardRef } from 'react';
import $ from 'jquery';
import queryString from 'query-string';
import './PlaceholderItem.scss';
import { semaphore } from '../../../util/async_loading_utils';

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
    const [item, setItem] = useState(undefined);
    const [error, setError] = useState(false);
    const { focusIndex } = props;
    const { worksheetUUID, onAsyncItemLoad, itemHeight } = props;
    const { directive, sort_keys } = props.item;
    useEffect(() => {
        (async function() {
            try {
                const { blocks } = await fetchData({ directive, worksheetUUID });
                setItem(blocks.length === 0 ? null : blocks[0]);
                if (blocks.length > 0) {
                    let actualBlock = blocks[0];
                    // replace with existing sort keys if there is one
                    if (sort_keys && actualBlock['sort_keys'] === undefined) {
                        actualBlock['sort_keys'] = sort_keys;
                    }
                    actualBlock.loadedFromPlaceholder = true;
                    if (
                        actualBlock['sort_keys'].length > 1 &&
                        actualBlock['sort_keys'][actualBlock['sort_keys'].length - 1] === null &&
                        focusIndex > 0
                    ) {
                        // When the last element in sort_keys is equal to "null" and there is more than one
                        // "sort key", this means that we have resolved an aggregate search directive such as
                        // "search .mine .count" that should be combined with the block right above it.
                        // An example of worksheet source that would trigger this condition:
                        //      ## **Basic statistics**
                        //      Number of bundles owned by me:
                        //      % search .mine .count
                        onAsyncItemLoad(focusIndex - 1, actualBlock);
                        onAsyncItemLoad(focusIndex, null);
                    } else {
                        onAsyncItemLoad(focusIndex, actualBlock);
                    }
                }
            } catch (e) {
                console.error(e);
                setError(e);
            }
        })();
        // TODO: see how we can add onAsyncItemLoad as a dependency, if needed.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [directive, worksheetUUID]);
    if (error) {
        return <div ref={ref}>Error loading item.</div>;
    }
    if (item === null) {
        // No items
        return <div ref={ref}>No results found.</div>;
    }
    return (
        <div
            ref={ref}
            className='codalab-item-placeholder'
            style={{ height: itemHeight || 100 }}
        ></div>
    );
});
