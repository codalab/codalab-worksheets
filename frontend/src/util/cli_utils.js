/**
 * This module allows easy access with the server's CLI to run commands.
 */

import $, { jQuery } from 'jquery';
import _ from 'underscore';

/**
 * @param command
 *     Codalab CLI command, without "cl". E.g. "run ...".
 * @param worksheet_uuid
 *     UUID of active worksheet. (6/3/2019: May be retrieved with ws.info.uuid). If
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
    })
        .done((data) => {
            // data := {
            //     structured_result: { ... },
            //     output: string,
            // }

            // The bundle service can respond with instructions back to the UI.
            // These come in the form of an array of 2-arrays, with the first element
            // representing the type of action, and the second element parameterizing
            // that action.
            //
            //    if (data.structured_result && data.structured_result.ui_actions) {
            //        _.each(data.structured_result.ui_actions, function(action) {
            //            self.doUIAction(action[0], action[1]);
            //        });
            //    }
            //
            // Possible actions and parameters:
            // 'openWorksheet', WORKSHEET_UUID  => load worksheet
            // 'setEditMode', true|false        => set edit mode
            // 'openBundle', BUNDLE_UUID]       => load bundle info in new tab
            // 'upload', null                   => open upload modal
            //
            // The code copied from the (now DEPRECATED) WorksheetActionBar component shows how this may be used.
            //
            // doUIAction(action, parameter) {
            //    var self = this;
            //    ({
            //        openWorksheet: function(uuid) {
            //            self.props.openWorksheet(uuid);
            //        },
            //        setEditMode: function(editMode) {
            //            self.props.editMode();
            //        },
            //        openBundle: function(uuid) {
            //            window.open('/bundles/' + uuid + '/', '_blank');
            //        },
            //        upload: function() {
            //            // Just switch focus to the upload button.
            //            self.props.setFocus(-1, null);
            //            self.terminal.focus(false);
            //            $('#upload-bundle-button').focus();
            //        },
            //    }[action](parameter));
            // }
            console.info(data);
        })
        .fail((jqXHR, status, error) => {
            // Some exception occurred outside of the CLI
            console.error(jqXHR.responseText);
        });
}