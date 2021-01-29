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
import { getQueryParams } from '../worksheets/NewUpload/NewUpload.jsx';
import $ from 'jquery';
import { createAlertText, getDefaultBundleMetadata } from '../../util/worksheet_utils';
import Avatar from '@material-ui/core/Avatar';

// TODO: ONLY SHOW THE EDITOR FOR AUTH USER
// TODO: ADD Field to DB; Update profile filed
// TODO: FOR AVATAR, fetch content from user info in DB

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

const getBase64 = (img, callback) => {
    const reader = new FileReader();
    reader.addEventListener('load', () => callback(reader.result));
    reader.readAsDataURL(img);
};

class EditableAvatar extends React.Component {
    handleClickOpen = () => {
        this.setState({ isOpen: true });
    };
    handleClose = () => {
        this.setState({ isOpen: false });
    };

    state = {
        isOpen: false,
        avatar: '',
        file: null,
    };

    handleChange = (file) => {
        file = file.target.files[0];
        getBase64(file, (url) => {
            this.setState({
                avatar: url,
                isOpen: true,
                file: file,
            });
        });
        return false;
    };

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

                // this.setState({
                //     avatar: dataUrl,
                // });

                return dataUrl;
            })
            .then((dataUrl) => {
                this.setState({
                    avatar: dataUrl,
                });
            })
            .catch(function(error) {
                alert(createAlertText(url, error.responseText));
            });
    }

    uploadImgAsync(bundleUuid, file, fileName) {
        return new Promise((resolve, reject) => {
            let reader = new FileReader();
            reader.onload = () => {
                let arrayBuffer = reader.result,
                    bytesArray = new Uint8Array(arrayBuffer);
                let url =
                    '/rest/bundles/' + bundleUuid + '/contents/blob/?' + getQueryParams(fileName);
                $.ajax({
                    url: url,
                    type: 'PUT',
                    contentType: 'application/octet-stream',
                    data: new Blob([bytesArray]),
                    processData: false,
                    success: function(data) {
                        resolve(data);
                    },
                    error: function(error) {
                        alert(createAlertText(url, error.responseText, 'refresh and try again.'));
                        this.props.onUploadFinish();
                        reject(error);
                    }.bind(this),
                });
            };
            reader.readAsArrayBuffer(file);
        });
    }

    // Upload the adjusted avatar
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
                headPhoto: this.editor.getImage().toDataURL(), // 编辑完成后的图片base64
                file: blob,
            });

            async function createImageBundle(url, data) {
                let result;
                try {
                    result = await $.ajax({
                        url: url,
                        data: data,
                        contentType: 'application/json',
                        type: 'POST',
                    });
                    return result;
                } catch (error) {
                    alert(createAlertText(url, error.responseText));
                }
            }
            // Fetch the uuid of user's dashboard worksheet for storing the profile-image bundle

            const response = await fetch(`/rest/worksheets?specs=dashboard`).then((e) => e.json());
            const worksheetUUID = response.data[0].id;
            const file = this.state.file;

            const fileName: string = this.props.userInfo.user_name + '.JPG';
            const createBundleData = getDefaultBundleMetadata(fileName);
            let url = `/rest/bundles?worksheet=${worksheetUUID}`;
            const bundle = await createImageBundle(url, JSON.stringify(createBundleData));
            const bundleUuid = bundle.data[0].id;
            await this.uploadImgAsync(bundleUuid, file, fileName);
            await this.fetchImg(bundleUuid);
        });
    };
    render() {
        const { classes } = this.props;
        return (
            <div className={classes.box}>
                <Avatar
                    className={classes.Avatar}
                    default={this.props.userInfo.user_name.charAt(0)}
                    src={this.state.avatar}
                />
                <Button variant='outlined' color='primary' onClick={this.handleClickOpen}>
                    Edit Avatar
                </Button>
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
        );
    }
}

export default withStyles(styles)(EditableAvatar);
