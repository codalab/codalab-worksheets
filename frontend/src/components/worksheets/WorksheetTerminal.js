import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import _ from 'underscore';
import 'jquery.terminal';

const TERMINAL_MINIMIZE_HEIGHT = 30;
let TERMINAL_DRAGHEIGHT = 350;
class WorksheetTerminal extends React.Component {
    /** Constructor. */

    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    componentDidMount() {
        var self = this;
        $('#dragbar_horizontal').mousedown(function(e) {
            self.resizePanel(e);
        });

        // really hacky way of making it so that the mousemove listener gets removed on mouseup
        $(document).mouseup(function(e) {
            $(document).unbind('mousemove');
        });

        // See JQuery Terminal API reference for more info about this plugin:
        // http://terminal.jcubic.pl/api_reference.php
        this.terminal = $('#command_line').terminal(
            async function(command, terminal) {
                if (command.length === 0) {
                    return;
                }

                var isEnabled = terminal.enabled();
                terminal.pause();
                try {
                    const data = await self.executeCommand(command);
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
                } catch (error) {
                    terminal.error(error.responseText);
                } finally {
                    terminal.resume();
                    if (!isEnabled) {
                        terminal.disable();
                    }
                    self.props.reloadWorksheet();
                }
            },
            {
                greetings:
                    "Click here to enter commands (e.g., help, run '<bash command>', rm <bundle>, kill <bundle>, etc.).",
                name: 'command_line',
                height: TERMINAL_MINIMIZE_HEIGHT,
                prompt: 'CodaLab> ',
                history: true,
                keydown: function(event, terminal) {
                    if (event.keyCode === 67 && event.ctrlKey) {
                        // 67 is 'c' keycode
                        terminal.exec('', false);
                        terminal.update(-1, terminal.get_prompt() + terminal.get_command());
                        terminal.set_command('');
                        event.preventDefault();
                    }
                    if (event.keyCode === 27) {
                        // esc
                        terminal.focus(false);
                    }
                },
                onBlur: function(term) {
                    if (term.data('resizing')) {
                        self.props.handleFocus();
                        return false;
                    }
                    if (term.enabled()) {
                        term.resize(term.width(), TERMINAL_MINIMIZE_HEIGHT);
                        self.props.handleBlur();
                    }
                },
                onFocus: function(term) {
                    if (!term.data('resizing')) {
                        term.resize(term.width(), TERMINAL_DRAGHEIGHT);
                    }
                    self.props.handleFocus();
                },
                completion: async function(lastToken, callback) {
                    var command = this.get_command();

                    const completions = await self.completeCommand(command);
                    callback(completions);
                },
            },
        );

        // Start with terminal focus off.
        this.terminal.focus(false);
    }

    renderHyperlinks(references) {
        Object.keys(references).forEach(function(key) {
            $('.terminal-output div div:contains(' + key + ')').html(function(idx, html) {
                var hyperlink_info = references[key];
                if (hyperlink_info.uuid) {
                    if (hyperlink_info.type === 'bundle' || hyperlink_info.type === 'worksheet') {
                        var link = '/' + hyperlink_info['type'] + 's/' + hyperlink_info['uuid'];
                        return html.replace(
                            key,
                            '<a href=' +
                                link +
                                " target='_blank' rel='noopener noreferrer'>" +
                                key +
                                '</a>',
                        );
                    } else {
                        console.warn(
                            "Couldn't create hyperlink for ",
                            hyperlink_info.uuid,
                            " . Type is neither 'worksheet' nor 'bundle'",
                        );
                    }
                } else {
                    console.warn('Complete uuid not available for ', key, ' to create hyperlink');
                }
            });
        });
    }

    doUIAction(action, parameter) {
        // Possible actions and parameters:
        // 'openWorksheet', WORKSHEET_UUID  => load worksheet
        // 'setEditMode', true|false        => set edit mode
        // 'openBundle', BUNDLE_UUID]       => load bundle info in new tab
        // 'upload', null                   => open upload modal
        var self = this;
        ({
            openWorksheet: function(uuid) {
                self.props.openWorksheet(uuid);
            },
            setEditMode: function(editMode) {
                self.props.editMode();
            },
            openBundle: function(uuid) {
                window.open('/bundles/' + uuid + '/', '_blank');
            },
        }[action](parameter));
    }
    executeCommand(command) {
        var self = this;

        // returns a jQuery Promise
        return $.ajax({
            type: 'POST',
            cache: false,
            url: '/rest/cli/command',
            contentType: 'application/json; charset=utf-8',
            dataType: 'json',
            data: JSON.stringify({
                worksheet_uuid: this.props.ws.info.uuid,
                command: command,
            }),
        })
            .done(function(data) {
                // data := {
                //     structured_result: { ... },
                //     output: string
                // }

                // The bundle service can respond with instructions back to the UI.
                // These come in the form of an array of 2-arrays, with the first element
                // representing the type of action, and the second element parameterizing
                // that action.
                if (data.structured_result && data.structured_result.ui_actions) {
                    _.each(data.structured_result.ui_actions, function(action) {
                        self.doUIAction(action[0], action[1]);
                    });
                }
            })
            .fail(function(jqXHR, status, error) {
                // Some exception occurred outside of the CLI
                console.error(jqXHR.responseText);
            });
    }
    completeCommand(command) {
        var deferred = $.Deferred();
        $.ajax({
            type: 'POST',
            cache: false,
            url: '/rest/cli/command',
            contentType: 'application/json; charset=utf-8',
            dataType: 'json',
            data: JSON.stringify({
                worksheet_uuid: this.props.ws.info.uuid,
                command: command,
                autocomplete: true,
            }),
            success: function(data, status, jqXHR) {
                deferred.resolve(data.completions);
            },
            error: function(jqHXR, status, error) {
                console.error(jqHXR.responseText);
                deferred.reject();
            },
        });
        return deferred.promise();
    }
    componentWillUnmount() {}
    componentDidUpdate() {}
    resizePanel(e) {
        var terminal = $('#ws_search');
        var topOffset = terminal.offset().top;
        var worksheetHeight = $('#worksheet').height();
        var commandLine = $('#command_line');
        $(document).mousemove(function(e) {
            e.preventDefault();
            $('#command_line').data('resizing', 'true');
            var terminalHeight = e.pageY - topOffset;
            var terminalHeightPercentage = (terminalHeight / worksheetHeight) * 100;
            if (65 < terminalHeight && terminalHeightPercentage < 90) {
                // minimum height: 65px; maximum height: 90% of worksheet height
                terminal.css('height', terminalHeight);
                TERMINAL_DRAGHEIGHT = terminalHeight - 20;
                commandLine.terminal().resize(commandLine.width(), TERMINAL_DRAGHEIGHT);
            }
        });
    }
    render() {
        return (
            <div id='ws_search' className={this.props.hidden ? 'search-hidden' : ''}>
                <div className=''>
                    <div id='command_line' />
                </div>
                <div id='dragbar_horizontal' className='dragbar' />
            </div>
        );
    }
}

export default WorksheetTerminal;
