import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import { renderSize, renderDuration } from '../util/worksheet_utils';

/**
 * This stateful component ___.
 */
class UserInfo extends React.Component<
    {
        /** React components within opening & closing tags. */
        children: React.Node,
    },
    {
        // Optional: type declaration of this.state.
    },
> {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    processData(response) {
        // Shim in links to change email and password
        var user = response.data;
        user.attributes.email = (
            <span>
                {user.attributes.email} <a href='/account/changeemail'>(change)</a>
            </span>
        );
        user.attributes.password = (
            <span>
                ******** <a href='/rest/account/reset'>(change)</a>
            </span>
        );
        return user;
    }

    componentDidMount() {
        console.log('UserInfo Mount');
        $.ajax({
            method: 'GET',
            url: '/rest/user',
            dataType: 'json',
        })
            .done((response) => {
                console.log(response);
                this.setState({
                    user: this.processData(response),
                });
            })
            .fail((xhr, status, err) => {
                this.setState({
                    errors: xhr.responseText,
                });
            });
    }

    /** Renderer. */
    render() {
        return <div />;
    }
}

class AccountNotificationsCheckbox extends React.Component<{
    user: {},
    errors: {},
    onChange: () => mixed,
    fieldKey: string,
    title: string,
}> {
    handleClick(cb) {
        var notifications = this.props.user.attributes['notifications'];
        this.props.onChange('notifications', parseInt(this.props.fieldKey));
    }

    render() {
        var inputId = 'account_profile_' + this.props.fieldKey;
        var notifications = this.props.user.attributes['notifications'];
        var checked = parseInt(this.props.fieldKey) === notifications;
        return (
            <div className='form-group row'>
                <label htmlFor={inputId} className='col-sm-9 form-control-label'>
                    {this.props.title}
                </label>
                <div className='col-sm-3'>
                    <input type='checkbox' checked={checked} onClick={this.handleClick} />
                </div>
            </div>
        );
    }
}

class AccountProfileField extends React.Component<
    {
        user: {},
        errors: {},
        onChange: () => mixed,
        fieldKey: string,
        title: string,
        readOnly: boolean,
        writeOnly: boolean,
    },
    {
        isUpdated: boolean,
    },
> {
    static defaultProps = {
        readOnly: false,
        writeOnly: false,
    };

    state = {
        isUpdated: false,
    };

    componentWillReceiveProps(nextProps) {
        // isUpdated should be true after the first successful update of this field
        var justChanged =
            nextProps.user.attributes[this.props.fieldKey] !==
            this.props.user.attributes[this.props.fieldKey];
        this.setState({
            isUpdated: this.state.isUpdated || justChanged,
        });
    }
    value() {
        return this.props.user.attributes[this.props.fieldKey];
    }
    error() {
        return this.props.errors[this.props.fieldKey];
    }
    handleKeyPress(event) {
        // Blur input on Enter, triggering onBlur
        if (event.charCode === 13) {
            $(event.target).blur();
        }
    }
    handleBlur(event) {
        // Submit the data on blur if changed, interpreting name_empty input as null
        var newValue = event.target.value || null;
        if (newValue !== this.value() || this.error()) {
            this.props.onChange(this.props.fieldKey, newValue);
        }
    }
    render() {
        var inputId = 'account_profile_' + this.props.fieldKey;

        var fieldElement;
        if (this.props.readOnly) {
            // Render values properly
            var value = this.value();
            var key = this.props.fieldKey;
            if (key === 'disk_quota' || key === 'disk_used') value = renderSize(value);
            else if (key === 'time_quota' || key === 'time_used') value = renderDuration(value);

            // Read-only fields only need a simple div
            fieldElement = <div>{value}</div>;
        } else {
            var formStateIcon;
            if (this.error()) {
                formStateIcon = (
                    <span>
                        <span className='glyphicon glyphicon-remove' aria-hidden='true' />
                        <span>&nbsp;Error</span>
                    </span>
                );
            } else if (this.state.isUpdated) {
                formStateIcon = (
                    <span>
                        <span className='glyphicon glyphicon-ok' aria-hidden='true' />
                        <span>&nbsp;Saved</span>
                    </span>
                );
            } else {
                formStateIcon = <span />;
            }

            fieldElement = (
                <div className='row'>
                    <div className='col-sm-9'>
                        <input
                            id={inputId}
                            className='form-control'
                            placeholder={this.props.title}
                            defaultValue={this.value()}
                            onBlur={this.handleBlur}
                            onKeyPress={this.handleKeyPress}
                        />
                    </div>
                    <div className='col-sm-3'>{formStateIcon}</div>
                </div>
            );
        }

        return (
            <div>
                <div className='form-group row'>
                    <label htmlFor={inputId} className='col-sm-3 form-control-label'>
                        {this.props.title}
                    </label>
                    <div className='col-sm-9'>
                        <div>{fieldElement}</div>
                        <div className='account-profile-error'>{this.error()}</div>
                    </div>
                </div>
            </div>
        );
    }
}

export default UserInfo;
