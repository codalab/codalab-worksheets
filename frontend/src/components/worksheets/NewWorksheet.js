import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import Button from '../Button';
import { createHandleRedirectFn, buildTerminalCommand } from '../../util/worksheet_utils';

var SAMPLE_WORKSHEET_TEXT = '-worksheetname';
var NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_\.\-]*$/i;

type Props = {
    clickAction: 'DEFAULT' | 'SIGN_IN_REDIRECT' | 'DISABLED',
    ws: {},
    userInfo?: {},
    escCount: number,
};

class NewWorksheet extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            showNewWorksheet: false,
            newWorksheetName: '',
        });
    }

    componentWillReceiveProps(nextProps) {
        if (nextProps.escCount != this.props.escCount && this.state.showNewWorksheet) {
            this.toggleNewWorksheet();
        }
        if (nextProps.userInfo != this.props.userInfo) {
            this.setState({
                newWorksheetName: nextProps.userInfo.user_name + SAMPLE_WORKSHEET_TEXT,
            });
        }
    }

    toggleNewWorksheet = () => {
        if (this.state.showNewWorksheet) {
            $('#new-worksheet').css('display', 'none');
            this.setState({
                newWorksheetName: this.props.userInfo.user_name + SAMPLE_WORKSHEET_TEXT,
            });
        } else {
            $('#new-worksheet').css('display', 'block');
            var inputVal = $('#new-worksheet-input').val();
            // highlight the second part of the suggested title for the user to change
            $('#new-worksheet-input')[0].setSelectionRange(
                inputVal.indexOf('-') + 1,
                inputVal.length,
            );
            $('#new-worksheet-input').focus();
        }
        this.setState({ showNewWorksheet: !this.state.showNewWorksheet });
    };

    handleNameChange = (event) => {
        var name = event.target.value;
        if (name.match(NAME_REGEX) != null || name === '') {
            this.setState({ newWorksheetName: event.target.value });
        }
    };

    createNewWorksheet = () => {
        if (this.state.newWorksheetName === '') {
            $('#new-worksheet-input').focus();
            return;
        }
        var args = ['new', this.state.newWorksheetName];
        $('#command_line')
            .terminal()
            .exec(buildTerminalCommand(args));
        this.toggleNewWorksheet();
    };

    handleKeyDown = (e) => {
        if (e.keyCode === 13) {
            // enter shortcut
            e.preventDefault();
            this.createNewWorksheet();
        } else if (e.keyCode === 27) {
            // esc shortcut
            e.preventDefault();
            this.toggleNewWorksheet();
        }
    };

    render() {
        var new_worksheet_name = (
            <input
                type='text'
                id='new-worksheet-input'
                value={this.state.newWorksheetName}
                onChange={this.handleNameChange}
                onKeyDown={this.handleKeyDown}
            />
        );

        var create_button = (
            <Button text='Create' type='primary' handleClick={this.createNewWorksheet} />
        );

        var cancel_button = (
            <Button text='Cancel' type='default' handleClick={this.toggleNewWorksheet} />
        );

        /*** creating newWorksheetButton ***/
        var typeProp, handleClickProp;
        switch (this.props.clickAction) {
            case 'DEFAULT':
                handleClickProp = this.toggleNewWorksheet;
                typeProp = 'primary';
                break;
            case 'SIGN_IN_REDIRECT':
                handleClickProp = createHandleRedirectFn(
                    this.props.ws.info ? this.props.ws.info.uuid : null,
                );
                typeProp = 'primary';
                break;
            case 'DISABLED':
                handleClickProp = null;
                typeProp = 'disabled';
                break;
            default:
                break;
        }

        var newWorksheetButton = (
            <Button
                text='New Worksheet'
                type={typeProp}
                width={120}
                handleClick={handleClickProp}
                flexibleSize={true}
            />
        );

        return (
            <div className='inline-block'>
                <div id='new-worksheet'>
                    <span className='close' onClick={this.toggleNewWorksheet}>
                        ×
                    </span>
                    <p className='pop-up-title'>New Worksheet</p>
                    {new_worksheet_name}
                    <p id='new-worksheet-message' className='pop-up-text'>
                        CodaLab>&nbsp;
                        <span className='pop-up-command'>cl new {this.state.newWorksheetName}</span>
                    </p>
                    <div id='new-worksheet-button'>
                        {cancel_button}
                        {create_button}
                    </div>
                </div>
                {newWorksheetButton}
            </div>
        );
    }
}

export default NewWorksheet;
