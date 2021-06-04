import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import _ from 'underscore';
import { renderSize, renderDuration } from '../util/worksheet_utils';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import { apiWrapper, defaultErrorHandler, getUser, updateUser } from '../util/apiWrapper';
import DeleteIcon from '@material-ui/icons/Delete';
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import Divider from '@material-ui/core/Divider';

/**
 * This stateful component ___.
 */
class UserInfo extends React.Component {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            user: null,
            errors: {},
        });
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
        const callback = (data) => {
            this.setState({
                user: this.processData(data),
            });
        };
        getUser()
            .then(callback)
            .catch(defaultErrorHandler);
    }

    handleChange = (key, value) => {
        // Clone and update locally
        var newUser = $.extend({}, this.state.user);
        newUser.attributes = {};
        newUser.attributes[key] = value;

        // Push changes to server
        const callback = (data) => {
            this.setState({
                user: this.processData(data),
            });
        };
        updateUser(newUser)
            .then(callback)
            .catch(defaultErrorHandler);
    };

    /** Renderer. */
    render() {
        return (
            <React.Fragment>
                <SubHeader title='My Account' />
                <ContentWrapper>
                    {!this.state.user && (
                        <div className='account-profile-error'>{_.values(this.state.errors)}</div>
                    )}
                    {this.state.user && (
                        <form className='account-profile-form'>
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Username'
                                fieldKey='user_name'
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Email'
                                fieldKey='email'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Password'
                                fieldKey='password'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='First Name'
                                fieldKey='first_name'
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Last Name'
                                fieldKey='last_name'
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Affiliation'
                                fieldKey='affiliation'
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Website URL'
                                fieldKey='url'
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Last Login (UTC)'
                                fieldKey='last_login'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Date Joined (UTC)'
                                fieldKey='date_joined'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Disk Quota (bytes)'
                                fieldKey='disk_quota'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Disk Used (bytes)'
                                fieldKey='disk_used'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Time Quota'
                                fieldKey='time_quota'
                                readOnly
                            />
                            <AccountProfileField
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Time Used'
                                fieldKey='time_used'
                                readOnly
                            />
                            <AccountNotificationsCheckbox
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Send me only critical updates about my account.'
                                fieldKey='1'
                            />
                            <AccountNotificationsCheckbox
                                {...this.props}
                                user={this.state.user}
                                errors={this.state.errors}
                                onChange={this.handleChange}
                                title='Send me general updates about new features (once a month).'
                                fieldKey='2'
                            />
                            <DeleteAccountDialog user={this.state.user} />
                        </form>
                    )}
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

class AccountNotificationsCheckbox extends React.Component {
    handleClick = () => {
        this.props.onChange('notifications', parseInt(this.props.fieldKey));
    };

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

class AccountProfileField extends React.Component {
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
    handleKeyPress = (event) => {
        // Blur input on Enter, triggering onBlur
        if (event.charCode === 13) {
            $(event.target).blur();
        }
    };
    handleBlur = (event) => {
        // Submit the data on blur if changed, interpreting name_empty input as null
        var newValue = event.target.value || null;
        if (newValue !== this.value() || this.error()) {
            this.props.onChange(this.props.fieldKey, newValue);
        }
    };
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

function DeleteAccountDialog(user) {
    const [open, setOpen] = React.useState(false);
    const [message, setMessage] = React.useState('');

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = () => {
        setOpen(false);
    };

    const deleteAccount = () => {
        if (message !== 'Delete my account') {
            alert(
                'Please type "Delete my account" to verify your intention to delete your account. Case sensitive.',
            );
            return;
        }
        setOpen(false);
        apiWrapper
            .executeCommand('ufarewell ' + user.user.id)
            .then((data) => {
                window.location.href = 'https://worksheets.codalab.org';
            })
            .catch((error) => alert(error));
    };

    return (
        <div>
            <Divider style={{ marginBottom: '30px' }} />
            <h2 style={{ color: '#bc3638' }}>Delete account</h2>
            <p>
                To be safe, you can only delete your account if you do not own any bundles,
                worksheets, or groups.
            </p>
            <Button
                variant='contained'
                startIcon={<DeleteIcon />}
                onClick={handleClickOpen}
                style={{ backgroundColor: '#bc3638', color: 'white' }}
            >
                Delete your account
            </Button>
            <Dialog open={open} onClose={handleClose} aria-labelledby='delete-account-title'>
                <DialogTitle id='delete-account-title'>
                    {' '}
                    Are you sure you want to delete your account?
                </DialogTitle>
                <DialogContent>
                    <DialogContentText>
                        <p>Once you delete your account, you can not return back.</p>
                        <p>
                            Please ensure that you do not own any bundles, worksheets, or groups
                            before deleting your account; otherwise the deletion will fail.
                        </p>
                        <br />
                    </DialogContentText>
                    <TextField
                        autoFocus
                        id='message'
                        label='To verify, type "Delete my account" below:'
                        onChange={(e) => setMessage(e.target.value)}
                        fullWidth
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={handleClose} color='primary'>
                        Cancel
                    </Button>
                    <Button onClick={deleteAccount} style={{ color: 'red' }}>
                        Confirm
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default UserInfo;
