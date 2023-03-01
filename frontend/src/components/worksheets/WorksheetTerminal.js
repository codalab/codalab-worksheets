import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import _ from 'underscore';
import 'jquery.terminal';
import { withStyles } from '@material-ui/core';
import { apiWrapper } from '../../util/apiWrapper';

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
                    console.error(error);
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
                    // TODO: Allow clicking outside Web CLI to close it (https://github.com/codalab/codalab-worksheets/issues/4378).
                    // if (term.enabled()) {
                    //     term.resize(term.width(), TERMINAL_MINIMIZE_HEIGHT);
                    //     self.props.handleBlur();
                    // }
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
        return apiWrapper
            .executeCommand(command, this.props.ws.info.uuid)
            .then(function(data) {
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
                return data;
            })
            .catch(function(exception) {
                return { exception };
            });
    }
    completeCommand(command) {
        return apiWrapper
            .completeCommand(command, this.props.ws.info.uuid)
            .then((data) => data.completions)
            .catch((error) => {
                console.error(error);
            });
    }
    componentWillUnmount() {}
    componentDidUpdate() {}
    render() {
        const { classes, hidden } = this.props;
        const activeClass = !hidden ? classes.terminalActive : '';
        return (
            <div id='ws_search' className={`${classes.terminalContainer} ${activeClass}`}>
                <div id='command_line' />
                <div id='dragbar_horizontal' className='dragbar' />
            </div>
        );
    }
}

const styles = () => ({
    terminalContainer: {
        position: 'fixed',
        zIndex: 10,
        width: '100%',
        height: 400,
        padding: '10px 0',
        marginTop: '-410px',
        background: 'rgba(245, 245, 245, 0.9)',
        boxShadow: '0 1px 10px 0 rgba(0,0,0,0.12), 0 2px 4px -1px rgba(0,0,0,0.4)',
        transition: 'margin-top 800ms',
    },
    terminalActive: {
        marginTop: 0,
    },
});

export default withStyles(styles)(WorksheetTerminal);
