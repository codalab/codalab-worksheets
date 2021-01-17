import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import { default as SideBar } from './SideBar';
import { default as MainPanel } from './MainPanel';
import $ from 'jquery';
import { withRouter } from 'react-router';

/**
 * This route page displays the new Dashboard, which is the landing page for all the users.
 */
class NewDashboard extends React.Component<{
    // ID of user.
    uid: string,
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
            authUid: null, // User Id of the current authenticated user
            ownDashboard: false, // Whether the dashboard is owned by current user
        };
    }

    componentDidMount() {
        $('body').addClass('ws-interface');
        // Fetch the current user's information
        $.ajax({
            url: '/rest/user',
            dataType: 'json',
            cache: false,
            type: 'GET',
            success: function(data) {
                let authUid: String = data.data.id;
                // Redirect to current user's own dashboard
                this.setState({ authUid: authUid });
                let ownDashboard: boolean;
                if (!this.props.uid) {
                    this.props.history.push('/dashboard/' + authUid);
                    ownDashboard = true;
                } else {
                    ownDashboard = authUid === this.props.uid;
                }

                $.ajax({
                    url: ownDashboard ? '/rest/user' : '/rest/users/' + this.props.uid,
                    dataType: 'json',
                    cache: false,
                    type: 'GET',
                    success: function(data) {
                        const userInfo = data.data.attributes;
                        userInfo.user_id = data.data.id;
                        this.setState({
                            userInfo: userInfo,
                            ownDashboard,
                        });
                    }.bind(this),
                    error: function(xhr, status, err) {
                        console.error(xhr.responseText);
                    },
                });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(xhr.responseText);
            },
        });
    }

    /** Renderer. */
    render() {
        if (this.state.userInfo) {
            return (
                <div>
                    <Grid container spacing={30}>
                        <Grid item xs={3}>
                            <SideBar
                                userInfo={this.state.userInfo}
                                showQuota={this.state.ownDashboard}
                            ></SideBar>
                        </Grid>
                        <Grid item xs>
                            <MainPanel userInfo={this.state.userInfo}></MainPanel>
                        </Grid>
                    </Grid>
                </div>
            );
        } else {
            return null;
        }
    }
}

export default NewDashboard;
export default withRouter(NewDashboard);
