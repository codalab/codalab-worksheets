import * as React from 'react';
import $ from 'jquery';
import Card from '@material-ui/core/Card';
import { withStyles } from '@material-ui/core/styles';
import Box from '@material-ui/core/Box';
import Button from '@material-ui/core/Button';
import { green } from '@material-ui/core/colors';
import NewWorksheetIcon from '@material-ui/icons/NoteAdd';
import Tooltip from '@material-ui/core/Tooltip';
import { NAME_REGEX } from '../../constants';
import { executeCommand } from '../../util/cli_utils';
import Dialog from '@material-ui/core/Dialog';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogContent from '@material-ui/core/DialogContent';
import TextField from '@material-ui/core/TextField';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogActions from '@material-ui/core/DialogActions';
/**
 * This route page displays the new Dashboard, which is the landing page for all the users.
 */
const kDefaultWorksheetName = 'unnamed';

const styles = ({ spacing, palette }) => {
    return {
        box: { marginLeft: 8, marginTop: 8, marginBottom: 8, backgroundColor: 'white' },
        titleBox: { marginLeft: 8, marginTop: 8, marginBottom: 8 },
        card: {
            display: 'flex',
            flexDirection: 'column',
            padding: spacing(2),
            elevation: 5,
            marginLeft: 8,
            width: '90%',
            borderRadius: 12,
            boxShadow: '0 2px 4px 0 rgba(138, 148, 159, 0.2)',
            '& > *:nth-child(1)': {
                marginRight: spacing(2),
            },
            '& > *:nth-child(2)': {
                flex: 'auto',
            },
        },
        wsCard: {
            display: 'flex',
            flexDirection: 'column',
            padding: spacing(2),
            elevation: 5,
            marginLeft: 8,
            marginBottom: 8,
            width: '90%',
            borderRadius: 12,
            boxShadow: '0 2px 4px 0 rgba(138, 148, 159, 0.2)',
            '& > *:nth-child(1)': {
                marginRight: spacing(2),
            },
            '& > *:nth-child(2)': {
                flex: 'auto',
            },
        },
        heading: {
            fontSize: 20,
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 500,
            letterSpacing: 0.15,
            lineHeight: '150%',
            marginBottom: 8,
            marginLeft: 8,
            float: 'left',
        },
        value: {
            fontFamily: 'Roboto',
            fontSize: 14,
            color: palette.grey[600],
            letterSpacing: 0.1,
            marginBottom: 4,
        },
        subheader: {
            fontSize: 16,
            marginBottom: 4,
            fontFamily: 'Roboto',
            fontWeight: 400,
            letterSpacing: 0.15,
            color: '#4153af',
            lineHeight: '200%',
        },
        button: {
            float: 'right',
            backgroundColor: green[500],
            color: 'white',
        },
    };
};

class MainPanel extends React.Component<{
    classes: {},
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            newWorksheetShowDialog: false,
            newWorksheetName: '',
            userInfo: null,
            snackbarShow: false,
            snackbarMessage: '',
            snackbarVariant: '',
            worksheets: [],
        };
    }

    componentDidMount() {
        const { classes } = this.props;
        // Fetch worksheets owned by the current user
        const worksheetUrl: URL = '/rest/interpret/wsearch';
        $.ajax({
            url: worksheetUrl,
            dataType: 'json',
            type: 'POST',
            cache: false,
            data: JSON.stringify({ keywords: ['.mine'] }),
            contentType: 'application/json; charset=utf-8',
            success: (data) => {
                const worksheets = data.response.map((ws, i) => (
                    <Card className={classes.wsCard}>
                        <Box className={classes.box} alignItems={'center'}>
                            <a className={classes.subheader} href={'/rest/worksheets/' + ws.uuid}>
                                {ws.title ? ws.title : 'Untitled'}
                            </a>
                            <Box display={'flex'} alignItems={'center'}>
                                <p className={classes.value}>
                                    {ws.name}{' '}
                                    {'   |   Owned by: ' + ws.owner_name + '[' + ws.owner_id + ']'}
                                </p>
                            </Box>
                        </Box>
                    </Card>
                ));
                this.setState({ worksheets });
            },
            error: (xhr, status, err) => {
                console.error(xhr.responseText);
            },
        });
    }

    static getDerivedStateFromProps(nextProps, prevState) {
        if (nextProps.userInfo !== prevState.userInfo) {
            return {
                userInfo: nextProps.userInfo,
                newWorksheetName: `${nextProps.userInfo.user_name}-`,
            };
        }
        return null;
    }
    resetDialog() {
        this.setState({
            newWorksheetShowDialog: false,
            newWorksheetName: `${this.state.userInfo.user_name}-`,
        });
    }

    createNewWorksheet() {
        this.resetDialog();
        if (!NAME_REGEX.test(this.state.newWorksheetName)) {
            this.setState({
                snackbarShow: true,
                snackbarMessage: `Names must match ${NAME_REGEX}, was ${this.state.newWorksheetName}`,
                snackbarVariant: 'error',
            });
            return;
        }

        executeCommand(`new ${this.state.newWorksheetName || kDefaultWorksheetName}`)
            .then((data) => {
                if (data.structured_result && data.structured_result.ui_actions) {
                    data.structured_result.ui_actions.forEach(([action, param]) => {
                        if (action === 'openWorksheet') {
                            window.location.href = `/worksheets/${param}`;
                        }
                    });
                }
            })
            .fail((error) => {
                this.setState({
                    snackbarShow: true,
                    snackbarMessage: error.responseText,
                    snackbarVariant: 'error',
                });
            });
    }
    /** Renderer. */
    render() {
        const { classes } = this.props;
        return (
            <div>
                <Card elevation={0} style={{ height: '100%', backgroundColor: '#f1f1f1' }}>
                    <Box className={classes.titleBox} display={'flex'} alignItems={'center'}>
                        <h3 className={classes.heading}>Worksheets</h3>
                        &nbsp;&nbsp;&nbsp;&nbsp;
                        <Tooltip title='New Worksheet'>
                            <Button
                                variant='contained'
                                className={classes.button}
                                startIcon={<NewWorksheetIcon />}
                                onClick={() => this.setState({ newWorksheetShowDialog: true })}
                            >
                                ADD
                            </Button>
                        </Tooltip>
                    </Box>

                    {this.state.worksheets}
                </Card>
                <Dialog
                    open={this.state.newWorksheetShowDialog}
                    onClose={() => this.resetDialog()}
                    aria-labelledby='form-dialog-title'
                    style={{ backgroundColor: '#f1f1f1' }}
                >
                    <DialogTitle id='form-dialog-title'>New Worksheet</DialogTitle>
                    <DialogContent>
                        <TextField
                            autoFocus
                            margin='dense'
                            id='name'
                            label='Name'
                            fullWidth
                            value={this.state.newWorksheetName}
                            placeholder={this.state.newWorksheetName}
                            onChange={(e) => this.setState({ newWorksheetName: e.target.value })}
                            onKeyDown={(e) => {
                                if (e.keyCode === 13) {
                                    // ENTER shortcut
                                    e.preventDefault();
                                    this.createNewWorksheet();
                                } else if (e.keyCode === 27) {
                                    // ESC shortcut
                                    e.preventDefault();
                                    this.resetDialog();
                                }
                            }}
                        />
                        <DialogContentText>
                            Note: worksheet names must be globally unique and not contain any
                            spaces.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={() => this.resetDialog()} color='primary'>
                            Cancel
                        </Button>
                        <Button onClick={() => this.createNewWorksheet()} color='primary'>
                            Confirm
                        </Button>
                    </DialogActions>
                </Dialog>
            </div>
        );
    }
}

export default withStyles(styles)(MainPanel);
