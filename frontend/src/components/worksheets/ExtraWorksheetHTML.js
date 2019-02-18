import * as React from 'react';

let ExtraWorksheetHTML = () => (
    <React.Fragment>
        <div id='save_progress' class='progress-message'>
            <img src='/img/Preloader_Small.gif' /> Saving...
        </div>
        <div id='update_progress' class='progress-message'>
            <img src='/img/Preloader_Small.gif' /> Updating...
        </div>
        <div id='save_error' class='progress-message'>
            <i class='glyphicon glyphicon-remove-circle' /> Error saving...
        </div>
        <div id='glossaryModal' class='modal' data-keyboard='true'>
            <div class='modal-dialog'>
                <div class='modal-content'>
                    <div class='modal-header'>
                        <button type='button' class='close' data-dismiss='modal'>
                            <span aria-hidden='true'>&times;</span>
                            <span class='sr-only'>Close</span>
                        </button>
                        <h4>Keyboard Shortcuts</h4>
                    </div>
                    <div class='modal-body'>
                        <a href='http://terminal.jcubic.pl/api_reference.php' target='_blank'>
                            {' '}
                            More shortcuts for web terminal (see Keyboard shortcuts section){' '}
                        </a>
                        <table class='table table-striped'>
                            <tbody>
                                <tr>
                                    <td>
                                        <kbd>c</kbd>
                                    </td>
                                    <td>Focus on web terminal</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>esc</kbd>
                                    </td>
                                    <td>Focus on worksheet</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>shift+c</kbd>
                                    </td>
                                    <td>Show/hide web terminal</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>e</kbd>
                                    </td>
                                    <td>Edit worksheet</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>shift+r</kbd>
                                    </td>
                                    <td>Refresh worksheet</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>j</kbd> or <kbd>↓</kbd>
                                    </td>
                                    <td>Move cursor down</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>k</kbd> or <kbd>↑</kbd>
                                    </td>
                                    <td>Move cursor up</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>shift+g</kbd>
                                    </td>
                                    <td>Move to end of worksheet</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>g g</kbd>
                                    </td>
                                    <td>Move to beginning of worksheet</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>enter</kbd>
                                    </td>
                                    <td>Open current bundle or worksheet (shift: new window)</td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>u</kbd>
                                    </td>
                                    <td>
                                        Paste UUID of current bundle or worksheet into web terminal
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>a</kbd>
                                    </td>
                                    <td>
                                        Paste arguments to recreate current bundle into web terminal
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        <kbd>?</kbd>
                                    </td>
                                    <td>Show keyboard shortcut help</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </React.Fragment>
);

export default ExtraWorksheetHTML;
