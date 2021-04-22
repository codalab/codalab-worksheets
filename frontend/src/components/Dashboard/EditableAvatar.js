import React from 'react';
import AvatarEditor from 'react-avatar-editor';
import 'rc-slider/assets/index.css';
import { withStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import MuiDialogTitle from '@material-ui/core/DialogTitle';
import MuiDialogContent from '@material-ui/core/DialogContent';
import MuiDialogActions from '@material-ui/core/DialogActions';
import IconButton from '@material-ui/core/IconButton';
import CloseIcon from '@material-ui/icons/Close';
import Typography from '@material-ui/core/Typography';
import $ from 'jquery';
import { createAlertText, getDefaultBundleMetadata } from '../../util/worksheet_utils';
import Avatar from '@material-ui/core/Avatar';
import ButtonBase from '@material-ui/core/ButtonBase';
import { post, updateUser, uploadImgAsync } from '../../util/apiWrapper';

const styles = (theme) => ({
    root: {
        margin: 10,
        padding: 16,
    },
    closeButton: {
        position: 'absolute',
        right: 8,
        top: 8,
        color: theme.palette.grey[500],
    },
    avatar: { marginLeft: 8, paddingRight: 8, marginTop: 12, marginBottom: 8 },
    box: {
        display: 'flex',
        marginLeft: 8,
        marginRight: 8,
        marginTop: 8,
        marginBottom: 0,
    },
    image: {
        position: 'relative',
        [theme.breakpoints.down('xs')]: {
            width: '100% !important', // Overrides inline-style
            height: 100,
        },
        '&:hover': {
            zIndex: 1,
            '& $imageBackdrop': {
                opacity: 0.6,
            },
            '& $imageTitle': {
                opacity: 0.8,
            },
        },
    },
    imageButton: {
        position: 'absolute',
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: theme.palette.common.white,
    },
    imageSrc: {
        position: 'absolute',
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
        backgroundSize: 'cover',
        backgroundPosition: 'center 40%',
    },
    imageBackdrop: {
        position: 'absolute',
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
        backgroundColor: theme.palette.common.black,
        opacity: 0,
        borderRadius: 30,
        transition: theme.transitions.create('opacity'),
    },
    imageTitle: {
        position: 'relative',
        opacity: 0,
    },
});

const DialogTitle = withStyles(styles)((props) => {
    const { children, classes, onClose, ...other } = props;
    return (
        <MuiDialogTitle disableTypography className={classes.root} {...other}>
            <Typography variant='h6'>{children}</Typography>
            {onClose ? (
                <IconButton aria-label='close' className={classes.closeButton} onClick={onClose}>
                    <CloseIcon />
                </IconButton>
            ) : null}
        </MuiDialogTitle>
    );
});

const DialogContent = withStyles((theme) => ({
    root: {
        padding: 16,
    },
}))(MuiDialogContent);

const DialogActions = withStyles((theme) => ({
    root: {
        margin: 0,
        padding: 8,
    },
}))(MuiDialogActions);

// Convert the image file to DataURL format
const getBase64 = (img, callback) => {
    const reader = new FileReader();
    reader.addEventListener('load', () => callback(reader.result));
    reader.readAsDataURL(img);
};

class EditableAvatar extends React.Component {
    componentDidMount() {
        if (this.props.userInfo.avatar_id) {
            // Fetch user's uploaded avatar image
            this.fetchImg(this.props.userInfo.avatar_id);
        }
    }

    handleClickOpen = () => {
        this.setState({ isOpen: true });
    };
    handleClose = () => {
        this.setState({ isOpen: false });
    };

    state = {
        isOpen: false, // Whether the avatar editor is open or not
        avatar: '', // DataURL of the avatar image
        file: null, // Newly uploaded avatar image
    };

    handleChange = (file) => {
        file = file.target.files[0];

        // Set limitation on avatar size
        // <= 5MB
        if (file.size > 5 * 1024 * 1024) {
            alert('Avatar size cannot exceed 5MB');
            this.setState({
                isOpen: false,
            });
            return;
        }

        getBase64(file, (url) => {
            this.setState({
                avatar: url,
                isOpen: true,
                file: file,
            });
        });
    };

    // Fetch the image file represented by the bundle
    fetchImg(bundleUuid) {
        // Set defaults
        let url = '/rest/bundles/' + bundleUuid + '/contents/blob/';

        fetch(url)
            .then(function(response) {
                if (response.ok) {
                    return response.arrayBuffer();
                }
                throw new Error('Network response was not ok.');
            })
            .then(function(data) {
                let dataUrl =
                    'data:image/png;base64,' +
                    btoa(
                        new Uint8Array(data).reduce(
                            (data, byte) => data + String.fromCharCode(byte),
                            '',
                        ),
                    );
                return dataUrl;
            })
            .then((dataUrl) => {
                // Update avatar shown on the page
                this.setState({
                    avatar: dataUrl,
                });
            })
            .catch(function(error) {
                console.log(url, error.responseText);
            });
    }

    // Store the bundle uuid of the avatar to database
    saveAvatarToDB(bundleUuid) {
        // Construct a user object for updating user information
        // Three fields needed: attributes/type/id
        const newUser = $.extend({}, this.props.userInfo);
        newUser.attributes = {};
        newUser.attributes['avatar_id'] = bundleUuid;
        newUser.type = 'users';
        newUser.id = this.props.userInfo.user_id;

        // Push changes to server
        updateUser(newUser);
    }

    // Callback function for avatar editor to upload the adjusted avatar
    submitAvatar = async () => {
        if (!this.editor.props.image) {
            // No image file chosen
            // Close the dialog
            this.setState({
                isOpen: false,
            });
            return;
        }

        this.editor.getImage().toBlob(async (blob) => {
            this.setState({
                isOpen: false,
                avatar: this.editor.getImage().toDataURL(),
                file: blob,
            });

            async function createImageBundle(url, data) {
                try {
                    return await post(url, data);
                } catch (error) {
                    alert(createAlertText(url, error.responseText));
                }
            }

            const file = this.state.file;
            // Rename the file to let it be related to the user
            const fileName: string = this.props.userInfo.user_name + '.JPG';
            // Create a detached bundle, so that no parent worksheet ID is needed for the avatar
            const bundle = await createImageBundle(
                `/rest/bundles?detached=1`,
                getDefaultBundleMetadata(fileName),
            );
            const bundleUuid = bundle.data[0].id;
            // Upload the avatar as a bundle
            const errorHandler = (error) => {
                alert(createAlertText(error, 'refresh and try again.'));
                this.props.onUploadFinish();
            };
            await uploadImgAsync(bundleUuid, file, fileName, errorHandler);
            // Store the bundle id to database
            await this.saveAvatarToDB(bundleUuid);
            // Fetch the new avatar from the bundle store by specifying the bundle id
            await this.fetchImg(bundleUuid);
        });
    };
    render() {
        const { classes } = this.props;
        const avatar = this.state.avatar ? (
            <Avatar className={classes.Avatar} src={this.state.avatar} />
        ) : (
            <Avatar className={classes.Avatar}> {this.props.userInfo.user_name.charAt(0)} </Avatar>
        );

        return (
            <div className={classes.box}>
                {this.props.ownDashboard ? (
                    <div>
                        <ButtonBase
                            focusRipple
                            onClick={this.handleClickOpen}
                            className={classes.image}
                        >
                            {avatar}
                            <span className={classes.imageBackdrop} />
                            <span className={classes.imageButton}>
                                <Typography
                                    component='span'
                                    variant='subtitle1'
                                    color='inherit'
                                    className={classes.imageTitle}
                                >
                                    {'edit'}
                                </Typography>
                            </span>
                        </ButtonBase>
                        <Dialog
                            onClose={this.handleClose}
                            aria-labelledby='customized-dialog-title'
                            open={this.state.isOpen}
                        >
                            <DialogTitle id='customized-dialog-title' onClose={this.handleClose}>
                                Change Your Profile Picture
                            </DialogTitle>
                            <DialogContent dividers>
                                <AvatarEditor
                                    ref={(editor) => {
                                        this.editor = editor;
                                    }}
                                    image={this.state.avatar}
                                    width={200}
                                    height={200}
                                    border={50}
                                    color={[0, 0, 0, 0.3]} // RGBA
                                />
                                <Button
                                    variant='contained'
                                    label='Upload an Image'
                                    labelPosition='before'
                                    containerElement='label'
                                >
                                    <input
                                        ref='in'
                                        type='file'
                                        accept='image/*'
                                        onChange={this.handleChange}
                                    />
                                </Button>
                            </DialogContent>
                            <DialogActions>
                                <Button autoFocus onClick={this.submitAvatar} color='primary'>
                                    Save
                                </Button>
                            </DialogActions>
                        </Dialog>
                    </div>
                ) : (
                    <div> {avatar} </div>
                )}
            </div>
        );
    }
}

export default withStyles(styles)(EditableAvatar);
