import $ from 'jquery';
export async function fetchInterpretedWorksheet(worksheetUuid, bundleUuids = []) {
    let data = {
        uuid: bundleUuids,
    };
    return await $.ajax({
        type: 'GET',
        url: '/rest/interpret/worksheet/' + worksheetUuid,
        dataType: 'json',
        cache: false,
        data,
        traditional: true, // Make query strings in format uuid=x&uuid=y&uuid=z
    });
}
