/**
 * This module allows easy access with the server's CLI to run commands.
 */

import $ from 'jquery';

/**
 * @param command
 *     Codalab CLI command, without "cl". E.g. "run ...".
 * @param worksheet_uuid
 *     UUID of active worksheet. (6/3/2019: May be retrieved with ws.info.uuid).
 * Example usage:
    executeCommand(command)
    .then(function(data) {
        if (data.output) {
            terminal.echo(data.output.replace(/\n$/, ''));
        }

        if (data.exception) {
            terminal.error(data.exception);
        }

        // Patch in hyperlinks to bundles
        if (data.structured_result && data.structured_result.refs) {
            self.renderHyperlinks(data.structured_result.refs);
        }
    })
    .fail(function(error) {
        terminal.error(error.responseText);
    })
    .always(function() {
        terminal.resume();
        if (!isEnabled) {
            terminal.disable();
        }
        self.props.reloadWorksheet();
    });
 */
export function executeCommand(command: string, worksheet_uuid?: string) {
    // returns a jQuery Promise
    return $.ajax({
        type: 'POST',
        cache: false,
        url: '/rest/cli/command',
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        data: JSON.stringify({
            worksheet_uuid: worksheet_uuid || null,
            command: command,
        }),
    });
}