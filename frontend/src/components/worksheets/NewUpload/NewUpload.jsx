// @flow
import React from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import JSZip from 'jszip';
import { CircularProgressbar, buildStyles } from 'react-circular-progressbar';
import 'react-circular-progressbar/dist/styles.css';
import { getDefaultBundleMetadata, createAlertText } from '../../../util/worksheet_utils';
import { FILE_SIZE_LIMIT_B, FILE_SIZE_LIMIT_GB } from '../../../constants';
import { createFileBundle, getQueryParams } from '../../../util/apiWrapper';
import axios from 'axios';

class NewUpload extends React.Component<{
    /** JSS styling object. */
    classes: {},

    /** The worksheet to insert into **/
    worksheetUUID: string,

    /** Insert after this bundle **/
    after_sort_key: string,
}> {
    state = {
        /* Whether the upload is in progress */
        uploading: false,
        percentComplete: 0,
    };

    inputFolder = React.createRef();
    inputFile = React.createRef();

    setFile = () => {
        const files = this.inputFile.current.files;

        if (!files.length) {
            return;
        }
        this.uploadFiles(files);
    };

    setFolder = () => {
        const files = this.inputFolder.current.files;
        if (!files.length) {
            return;
        }
        this.uploadFolder(files);
    };

    readFileAsync(bundleUuid, file) {
        return new Promise((resolve, reject) => {
            let reader = new FileReader();
            reader.onload = () => {
                let arrayBuffer = reader.result,
                    bytesArray = new Uint8Array(arrayBuffer);
                let url =
                    '/rest/bundles/' + bundleUuid + '/contents/blob/?' + getQueryParams(file.name);
                const config = {
                    onUploadProgress: (evt) => {
                        if (evt.lengthComputable) {
                            const percentComplete = parseInt((evt.loaded / evt.total) * 100);
                            this.setState({ percentComplete });
                        }
                    },
                    contentType: 'application/octet-stream',
                };
                axios
                    .put(url, new Blob([bytesArray]), config)
                    .then(({ data }) => resolve(data))
                    .catch((error) => {
                        this.clearProgress();
                        alert(createAlertText(error.responseText, 'refresh and try again.'));
                        this.props.onUploadFinish();
                        reject(error);
                    });
            };
            reader.readAsArrayBuffer(file);
        });
    }

    asyncUploadFiles = async (files) => {
        const { worksheetUUID, after_sort_key, focusedItem } = this.props;
        const { name, description } = this.state;
        this.setState({
            uploading: true,
        });

        let promises = [...files].map(async (file) => {
            const createBundleData = getDefaultBundleMetadata(name || file.name, description);
            let url = `/rest/bundles?worksheet=${worksheetUUID}`;
            // after_sort_key can be equal to 0
            if (after_sort_key || after_sort_key === 0) {
                url += `&after_sort_key=${after_sort_key}`;
            }
            // current focused item is an image block
            // pass after_image to the backend to make the backend add a blank line after the image block in the worksheet source to separate the newly uploaded files from the image block
            if (focusedItem && focusedItem.mode === 'image_block') {
                url += `&after_image=1`;
            }
            const errorHandler = (error) => {
                this.clearProgress();
                alert(createAlertText(url, error.responseText));
            };
            const bundle = await createFileBundle(url, createBundleData, errorHandler);
            const bundleUuid = bundle.data[0].id;

            const promise = await this.readFileAsync(bundleUuid, file);
            return promise;
        });

        await Promise.all(promises);
        const moveIndex = true;
        const param = { moveIndex };
        this.clearProgress();
        this.props.reloadWorksheet(undefined, undefined, param);
        this.props.onUploadFinish();
    };

    uploadFiles = (files) => {
        if (!files) {
            return;
        }

        let fileSize = 0;
        for (const file of files) {
            fileSize += file.size;
        }

        if (fileSize > FILE_SIZE_LIMIT_B) {
            alert(
                'File size is large than ' +
                    FILE_SIZE_LIMIT_GB +
                    'GB. Please upload your file(s) through CLI.',
            );
            return;
        }

        // let promises = [];

        this.asyncUploadFiles(files);
    };

    uploadFolder = async (files) => {
        if (!files) {
            return;
        }
        if (files.length > 1000) {
            alert(
                'There are too many files in the folder. Please zip your folder and upload again.',
            );
            return;
        }

        const { worksheetUUID, after_sort_key, focusedItem } = this.props;
        const { name, description } = this.state;
        const folderNamePos = files[0].webkitRelativePath.indexOf('/');
        let folderName = '';
        if (folderNamePos !== -1) {
            folderName = files[0].webkitRelativePath.slice(0, folderNamePos);
        }

        const createBundleData = getDefaultBundleMetadata(name || folderName + '.zip', description);
        this.setState({
            uploading: true,
        });
        let url = `/rest/bundles?worksheet=${worksheetUUID}`;
        url += `&after_sort_key=${isNaN(after_sort_key) ? -1 : after_sort_key}`;

        if (focusedItem && focusedItem.mode === 'image_block') {
            url += `&after_image=1`;
        }

        let zip = new JSZip();
        [...files].map((file) => {
            zip.file(file.webkitRelativePath, file);
        });

        const bytesArray = await zip.generateAsync({ type: 'uint8array', compression: 'DEFLATE' });
        const errorHandler = (error) => {
            this.clearProgress();
            alert(createAlertText(url, error.responseText));
        };
        const data = await createFileBundle(url, createBundleData, errorHandler);

        const bundleUuid = data.data[0].id;
        url =
            '/rest/bundles/' +
            bundleUuid +
            '/contents/blob/?' +
            getQueryParams(folderName + '.zip');
        try {
            const config = {
                onUploadProgress: (evt) => {
                    if (evt.lengthComputable) {
                        const percentComplete = parseInt((evt.loaded / evt.total) * 100);
                        this.setState({ percentComplete });
                    }
                },
                contentType: 'application/octet-stream',
            };
            await axios.put(url, new Blob([bytesArray]), config);
            this.clearProgress();
            const moveIndex = true;
            const param = { moveIndex };
            this.props.reloadWorksheet(undefined, undefined, param);
            this.props.onUploadFinish();
        } catch (error) {
            this.clearProgress();
            alert(createAlertText(url, error, 'refresh and try again.'));
            this.props.onUploadFinish();
        }
    };

    clearProgress = () => {
        this.setState({ numeratorComplete: 0, denominatorComplete: 0, uploading: false });
    };

    render() {
        const { classes } = this.props;
        const { percentComplete, uploading } = this.state;

        return (
            <React.Fragment>
                <input
                    key={this.props.after_sort_key} // force the NewUpload to update when the focused item changes
                    type='file'
                    id='codalab-file-upload-input'
                    multiple={true}
                    style={{ display: 'none' }}
                    ref={this.inputFile}
                    onChange={this.setFile}
                />
                <input
                    id='codalab-dir-upload-input'
                    type='file'
                    directory='true'
                    webkitdirectory='true'
                    mozdirectory='true'
                    style={{ display: 'none' }}
                    ref={this.inputFolder}
                    onChange={this.setFolder}
                />
                {uploading && (
                    <CircularProgressbar
                        className={classes.progress}
                        variant='determinate'
                        value={percentComplete}
                        text={`${percentComplete}% uploaded`}
                        styles={buildStyles({
                            textSize: '12px',
                        })}
                    />
                )}
            </React.Fragment>
        );
    }
}

const styles = (theme) => ({
    progress: {
        zIndex: 1000,
        position: 'fixed',
        left: '50vw',
        top: '50vh',
        width: 110,
        height: 110,
        transform: 'translateX(-50%) translateY(-50%)',
    },
});

export default withStyles(styles)(NewUpload);
