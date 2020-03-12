import React, { useState, useEffect, forwardRef } from 'react';
import $ from "jquery";
import queryString from "query-string";
import './PlaceholderItem.scss';
import { BLOCK_TO_COMPONENT } from '../WorksheetItemList';

const fetchData = async ({ worksheetUUID, directive }) => {
    const queryParams = {
        directive
    };
    const info = await $.ajax({
        type: 'GET',
        url: '/rest/interpret/worksheet/' + worksheetUUID + '?' + queryString.stringify(queryParams),
        async: true,
        dataType: 'json',
        cache: false
    });
    return info;
};

export default forwardRef((props, ref) => {
    const [item, setItem] = useState(null);
    const [error, setError] = useState(false);
    useEffect(() => {
        (async function () {
            console.log("running effect for item ", props.item);
            const { worksheetUUID } = props;
            const { directive } = props.item;
            try {
                const { items } = await fetchData({ directive, worksheetUUID });
                setItem(items[0]);
            } catch (e) {
                console.error(e);
                setError(e);
            }
        })();
    }, []);
    if (error) {
        return <div ref={ref}>Error loading item.</div>;
    }
    if (!item) {
        return <div ref={ref} className='codalab-item-placeholder'></div>;
    }
    const Comp = BLOCK_TO_COMPONENT[item.mode];
    return <Comp {...props} item={item} ref={ref} />;
});
