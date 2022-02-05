import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import { default as SideBar } from './SideBar';
import { default as MainPanel } from './MainPanel';
import $ from 'jquery';
import { withRouter } from 'react-router';
import { defaultErrorHandler, getUser, getUsers } from '../../util/apiWrapper';
import ErrorMessage from '../worksheets/ErrorMessage';

/**
 * This route page displays the new Dashboard, which is the landing page for all the users.
 */
class NewDashboard extends React.Component<{
    // ID of user.
    username: string,
    classes: {},
    auth: {
        isAuthenticated: boolean,
    },
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        const { auth } = this.props;
        if (!auth.isAuthenticated) {
            // Redirect to Login page if not authenticated
            this.props.history.push('/account/login');
        }
        this.state = {
            userInfo: null, // User info of the current user. (null is the default)
            authUsername: null, // User Id of the current authenticated user
            ownDashboard: false, // Whether the dashboard is owned by current user
        };
    }

    componentDidMount() {
        $('body').addClass('ws-interface');
        // Fetch the current user's information
        const callback = (data) => {
            let authUsername = data.data.attributes.user_name;
            // Redirect to current user's own dashboard
            this.setState({ authUsername: authUsername });
            let ownDashboard;
            if (!this.props.username) {
                this.props.history.push('/users/' + authUsername);
                ownDashboard = true;
            } else {
                ownDashboard = authUsername === this.props.username;
            }
            const callback = (data) => {
                const userInfo = data.data.attributes;
                userInfo.user_id = data.data.id;
                this.setState({
                    userInfo: userInfo,
                    ownDashboard,
                });
            };
            if (ownDashboard) {
                getUser().then(callback);
            } else {
                getUsers(this.props.username).then(callback);
            }
        };
        getUser()
            .then(callback)
            .catch(defaultErrorHandler);
    }

    /** Renderer. */
    render() {
        console.log(this.state.userInfo);
        console.log(this.state.userInfo && this.state.userInfo.has_access == 'False');
        if (this.state.userInfo && this.state.userInfo.has_access === 'False') {
            return (
                <ErrorMessage
                    message={'No access to this server, please contact the administrators.'}
                />
            );
        } else if (this.state.userInfo) {
            return (
                <div>
                    <Grid container spacing={30}>
                        <Grid item xs={3}>
                            <SideBar
                                userInfo={this.state.userInfo}
                                ownDashboard={this.state.ownDashboard}
                            ></SideBar>
                        </Grid>
                        <Grid item xs style={{ backgroundColor: '#f1f1f1' }}>
                            <MainPanel
                                userInfo={this.state.userInfo}
                                ownDashboard={this.state.ownDashboard}
                            ></MainPanel>
                        </Grid>
                    </Grid>
                </div>
            );
        } else {
            return null;
        }
    }
}

export default withRouter(NewDashboard);
