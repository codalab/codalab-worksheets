// @flow
import * as React from 'react';
import * as $ from 'jquery';
import classNames from 'classnames';
import Editable from 'react-x-editable';
import { renderFormat, serializeFormat } from '../util/worksheet_utils';

function isAscii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}

export class EditableField extends React.Component<{
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
        canEdit: true,
    };

    shouldComponentUpdate(nextProps, nextState) {
        return (nextProps.value !== this.props.value || nextProps.canEdit !== this.props.canEdit);
    }

    render() {
        return (
            <Editable
                dataType='text'
                mode='inline'
                value={this.props.value}
                disabled={!this.props.canEdit}
                emptyValueText='<none>'
                showButtons={false}
                validate={(value) => (isAscii(value) ? null : 'Only ASCII characters allowed.')}
                handleSubmit={({ value }) =>
                    $.ajax({
                        type: this.props.method,
                        url: this.props.url,
                        data: JSON.stringify(this.props.buildPayload(value)),
                        contentType: 'application/json; charset=UTF-8',
                        dataType: 'json',
                        cache: false,
                        context: this, // automatically bind `this` in all callbacks
                        xhr: function() {
                            // Hack for IE < 9 to use PATCH method
                            return window.XMLHttpRequest == null ||
                                new window.XMLHttpRequest().addEventListener == null
                                ? new window.ActiveXObject('Microsoft.XMLHTTP')
                                : $.ajaxSettings.xhr();
                        },
                    })
                        .done(function(response) {
                            if (this.props.onChange) this.props.onChange();
                        })
                        .fail(function(response, status, err) {
                            // TODO: this doesn't stop the value from updating in the frontend
                            console.log('Invalid value entered: ', response.responseText);
                        })
                }
            />
        );
    }
}

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
