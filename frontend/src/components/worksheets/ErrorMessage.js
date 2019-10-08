import * as React from 'react';
import { Link } from 'react-router-dom';
import Button from '@material-ui/core/Button';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';

class ErrorMessage extends React.Component{
    render() {
        return <Grid container direction="column" justify="center" alignItems="center" style={{ marginTop: 100 }}>
                    <Grid className='alert alert-danger alert-dismissable'>
                        <Grid item style={{ fontSize: '120%' }}> 
                            {this.props.message}
                        </Grid>
                        <Grid container direction='row' justify="center" alignItems="center" >
                            <Link to='/home'>
                                <Button color="primary" variant='contained'>
                                    Home
                                </Button>
                            </Link>
                            <Link to='/worksheets?name=dashboard' style={{ padding:10 }}>
                                <Button color="primary" variant='contained'>
                                    Dashboard
                                </Button>
                            </Link>
                        </Grid>
                    </Grid>  
                </Grid>
    }
}


export default ErrorMessage;
