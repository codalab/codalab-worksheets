import * as React from 'react';
import { Link } from 'react-router-dom';
import Button from '@material-ui/core/Button';
import Grid from '@material-ui/core/Grid';

class ErrorMessage extends React.Component{
    render() {
        return <Grid container 
            direction="column"
            justify="center"
            alignItems="center" 
            style={{ marginTop: 100 }}>
            <Grid>
                {this.props.message}
            </Grid>
            <Grid>
                <Link to='/worksheets?name=dashboard' style={{ padding:10 }}>
                    <Button color="primary" variant='contained'>
                        Dashboard
                    </Button>
                </Link>
                <Link to='/home'>
                        <Button color="primary" variant='contained'>
                            Home
                        </Button>
                </Link>
            </Grid>
        </Grid>
    }
}

export default ErrorMessage;
