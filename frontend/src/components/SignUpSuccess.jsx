import React from 'react';
import queryString from 'query-string';
import { withStyles } from '@material-ui/core/styles';
import { Typography } from '@material-ui/core';

class SignUpSuccess extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { email } = queryString.parse(this.props.location.search);
        const classes = this.props.classes;
        return (
            <div className={classes.outterContainer}>
                <div className={classes.innerContainer}>
                    <div className={classes.tada}>🎉</div>
                    <Typography variant='h5' gutterBottom>
                        Thank you for signing up for a CodaLab account!
                    </Typography>
                    <Typography classes={{ root: classes.subtitle }} variant='subtitle1'>
                        A link to verify your account has been sent to {email}.
                    </Typography>
                </div>
            </div>
        );
    }
}

const styles = (theme) => ({
    outterContainer: {
        display: 'flex',
        justifyContent: 'center',
        marginTop: 50,
    },
    innerContainer: {
        display: 'inline-block',
        padding: '15px 30px 22px',
        borderRadius: 12,
        boxShadow: theme.boxShadow.card,
        backgroundColor: 'white',
        textAlign: 'center',
    },
    subtitle: {
        color: theme.color.grey.darkest,
    },
    tada: {
        fontSize: 72,
    },
});

export default withStyles(styles)(SignUpSuccess);
