import * as React from 'react';
import { Link } from 'react-router-dom';
import Button from '@material-ui/core/Button';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';

class ErrorMessage extends React.Component {
    render() {
        return (
            <Grid
                container
                direction='column'
                justify='center'
                alignItems='center'
                style={{ marginTop: 100 }}
            >
                <Grid className='alert alert-danger alert-dismissable'>
                    <Grid item style={{ fontSize: '16px', marginLeft: 10 }}>
                        {this.props.message}
                    </Grid>
                </Grid>
            </Grid>
        );
    }
}

export default ErrorMessage;
