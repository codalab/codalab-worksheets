// @flow
import * as React from 'react';
import * as $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import { renderFormat, serializeFormat } from '../util/worksheet_utils';

const KEYCODE_ESC = 27;

function isAscii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}

class EditableFieldBase extends React.Component<{
    value: string,
    buildPayload: (string) => {},
    method: string,
    url: string,
    canEdit?: boolean,
    onChange?: () => void,
}> {
    /** Prop default values. */
    static defaultProps = {
        method: 'POST',
        canEdit: false,
    };

    constructor(props) {
        super(props);
        this.state = {
            editing: false,
            value: this.props.value,
            initValue: this.props.value,
            isValid: true,
        };
    }

    static getDerivedStateFromProps(nextProps, prevState) {
        if (nextProps.value !== prevState.initValue) {
            return { value: nextProps.value, initValue: nextProps.value };
        } else return null;
    }

    onClick = () => {
        this.setState({ editing: true });
    };

    onBlur = (event) => {
        if (!this.state.isValid) {
            event.preventDefault();
            return false;
        }

        this.setState({ editing: false });
        event.preventDefault();

        $.ajax({
            type: this.props.method,
            url: this.props.url,
            data: JSON.stringify(this.props.buildPayload(this.state.value)),
            contentType: 'application/json; charset=UTF-8',
            dataType: 'json',
            cache: false,
            context: this, // automatically bind `this` in all callbacks
            xhr: function() {
                // Hack for IE < 9 to use PATCH method
                return window.XMLHttpRequest === null ||
                    new window.XMLHttpRequest().addEventListener === null
                    ? new window.ActiveXObject('Microsoft.XMLHTTP')
                    : $.ajaxSettings.xhr();
            },
        })
            .done(function(response) {
                if (this.props.onChange) this.props.onChange(this.state.value);
            })
            .fail(function(response, status, err) {
                // TODO: this doesn't stop the value from updating in the frontend
                console.log('Invalid value entered: ', response.responseText);
            });
    };

    handleKeyPress = (event) => {
        if (event.keyCode === KEYCODE_ESC) {
            this.setState({ editing: false, value: this.state.initValue });
        }
    };

    handleAsciiChange = (event) => {
        // only ascii
        this.setState({ value: event.target.value, isValid: isAscii(event.target.value) });
    };

    handleFreeChange = (event) => {
        // allows non-ascii
        this.setState({ value: event.target.value });
    };

    shouldComponentUpdate(nextProps, nextState) {
        return (
            nextProps.value !== this.props.value ||
            nextState.value !== this.state.value ||
            nextProps.canEdit !== this.props.canEdit ||
            this.state.editing !== nextState.editing
        );
    }

    render() {
        if (!this.state.editing) {
            return (
                <span
                    className='editable-field'
                    onClick={this.onClick}
                    style={{ color: '#225ea8' }}
                >
                    {this.state.value === '' ? '<none>' : this.state.value}
                </span>
            );
        } else {
            return (
                <form onSubmit={this.onBlur}>
                    <input
                        type='text'
                        autoFocus
                        value={this.state.value}
                        onBlur={this.onBlur}
                        onChange={
                            this.props.allowASCII ? this.handleFreeChange : this.handleAsciiChange
                        }
                        onKeyDown={this.handleKeyPress}
                        placeholder={this.props.placeholder}
                        maxLength='259'
                        style={{
                            textOverflow: 'ellipsis',
                            whiteSpace: 'pre',
                            maxWidth: '100%',
                            minWidth: '65px',
                            padding: '0 4px 0 3px',
                            color: '#225ea8',
                        }}
                    />
                    {!this.state.isValid && (
                        <div style={{ color: '#a94442' }}>Only ASCII characters allowed.</div>
                    )}
                </form>
            );
        }
    }
}

const efStyles = (theme) => ({
    editableLinkContainer: {
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
    },
    editableLink: {
        textDecoration: 'none',
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
});

export const EditableField = withStyles(efStyles)(EditableFieldBase);

export class WorksheetEditableField extends React.Component<{
    uuid: string,
    fieldName: string,
}> {
    buildPayload(value) {
        return {
            data: [
                {
                    id: this.props.uuid,
                    type: 'worksheets',
                    attributes: {
                        [this.props.fieldName]: value,
                    },
                },
            ],
        };
    }

    render() {
        return (
            <EditableField
                {...this.props}
                url='/rest/worksheets'
                method='PATCH'
                buildPayload={(value) => this.buildPayload(value)}
            />
        );
    }
}

WorksheetEditableField.defaultProps = {
    allowASCII: false,
};

export class BundleEditableField extends React.Component<{
    value: any,
    uuid: string,
    fieldName: string,
    dataType: string,
}> {
    /** Prop default values. */
    static defaultProps = {
        dataType: 'string',
    };

    buildPayload(value) {
        return {
            data: [
                {
                    id: this.props.uuid,
                    type: 'bundles',
                    attributes: {
                        metadata: {
                            [this.props.fieldName]: serializeFormat(value, this.props.dataType),
                        },
                    },
                },
            ],
        };
    }
    render() {
        return (
            <EditableField
                {...this.props}
                value={renderFormat(this.props.value, this.props.dataType)}
                url='/rest/bundles'
                method='PATCH'
                buildPayload={(value) => this.buildPayload(value)}
            />
        );
    }
}
