// @flow
import React from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import JSZip from 'jszip';
import { CircularProgressbar, buildStyles } from 'react-circular-progressbar';
import "react-circular-progressbar/dist/styles.css";
import {
    createDefaultBundleName,
    pathIsArchive,
    getArchiveExt,
    getDefaultBundleMetadata,
    createAlertText,
} from '../../../util/worksheet_utils';
import {FILE_SIZE_LIMIT_B, FILE_SIZE_LIMIT_GB} from '../../../constants';

 export function getQueryParams(filename) {
    const formattedFilename = createDefaultBundleName(filename);
    const queryParams = {
        finalize: 1,
        filename: pathIsArchive(filename)
            ? formattedFilename + getArchiveExt(filename)
            : formattedFilename,
        unpack: pathIsArchive(filename) ? 1 : 0,
    };
    return $.param(queryParams);
}

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
        numeratorComplete: 0,
        denominatorComplete: 0,
    }

    inputFolder = React.createRef();
    inputFile = React.createRef();

    setFile = () => {
        const files = this.inputFile.current.files;

        if (!files.length) {
            return;
        }
        this.uploadFiles(files);
    }

    setFolder = () => {
        const files = this.inputFolder.current.files;
        if (!files.length) {
            return;
        }
        this.uploadFolder(files);
    }

    readFileAsync(bundleUuid, file) {
        return new Promise((resolve, reject) => {
            let reader = new FileReader();
            reader.onload = () => {
                let arrayBuffer = reader.result,
                    bytesArray = new Uint8Array(arrayBuffer);
                let url =
                    '/rest/bundles/' +
                    bundleUuid +
                    '/contents/blob/?' +
                    getQueryParams(file.name);
                $.ajax({
                    url: url,
                    type: 'PUT',
                    contentType: 'application/octet-stream',
                    data: new Blob([bytesArray]),
                    processData: false,
                    xhr: () => {
                        let xhr = new window.XMLHttpRequest();
                        xhr.upload.addEventListener(
                            'progress',
                            (evt) => {
                                if (evt.lengthComputable) {
                                    this.setState(prevState => {
                                        return { numeratorComplete: prevState.numeratorComplete + evt.loaded,
                                            denominatorComplete: prevState.denominatorComplete + evt.total};
                                    });
                                }
                            },
                            false,
                        );
                        return xhr;
                    },
                    success: function (data) {
                        resolve(data);
                    },
                    error: function (error) {
                        this.clearProgress();
                        alert(
                            createAlertText(
                                error.responseText,
                                'refresh and try again.',
                            ),
                        );
                        this.props.onUploadFinish();
                        reject(error);
                    }.bind(this),
                });

            };
            reader.readAsArrayBuffer(file);
        })
    }

    asyncUploadFiles = async (files) => {
        const { worksheetUUID, after_sort_key } = this.props;
        const { name, description } = this.state;
        this.setState({
            uploading: true,
        });

        let promises = [...files].map(async file => {
            const createBundleData = getDefaultBundleMetadata(name || file.name, description);
            let url = `/rest/bundles?worksheet=${ worksheetUUID }`;
            // after_sort_key can be equal to 0
            if (after_sort_key || after_sort_key === 0) {
                url += `&after_sort_key=${ after_sort_key }`;
            }
            async function createFileBundle(url, data) {
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
                    this.clearProgress();
                    alert(createAlertText(url, error.responseText));
                }
            }

            const bundle = await createFileBundle(url, JSON.stringify(createBundleData));
            const bundleUuid = bundle.data[0].id;

            const promise = await this.readFileAsync(bundleUuid, file);
            return promise;
        })

        await Promise.all(promises);
        const moveIndex = true;
        const param = { moveIndex };
        this.clearProgress();
        this.props.reloadWorksheet(undefined, undefined, param);
        this.props.onUploadFinish();
    }

    uploadFiles = (files) => {
        if (!files) {
            return;
        }

        let fileSize = 0;
        for (const file of files) {
            fileSize += file.size;
        }

        if (fileSize > FILE_SIZE_LIMIT_B) {
            alert('File size is large than ' + FILE_SIZE_LIMIT_GB + 'GB. Please upload your file(s) through CLI.');
            return;
        }

        // let promises = [];

        this.asyncUploadFiles(files);
    }

    uploadFolder = async (files) => {
        if (!files) {
            return;
        }
        if (files.length > 1000) {
            alert('There are too many files in the folder. Please zip your folder and upload again.');
            return;
        }

        const { worksheetUUID, after_sort_key } = this.props;
        const { name, description } = this.state;
        const folderNamePos = files[0].webkitRelativePath.indexOf("/");
        let folderName = "";
        if (folderNamePos !== -1) {
            folderName = files[0].webkitRelativePath.slice(0, folderNamePos)
        }

        const createBundleData = getDefaultBundleMetadata(name || folderName + ".zip", description);
        this.setState({
            uploading: true,
        });
        let url = `/rest/bundles?worksheet=${ worksheetUUID }`;
        url += `&after_sort_key=${ isNaN(after_sort_key) ? -1: after_sort_key }`;


        let zip = new JSZip();
        [...files].map(file => {
            zip.file(file.webkitRelativePath, file);
        });

        const bytesArray = await zip.generateAsync({type:"uint8array", compression: "DEFLATE"});
        let data;
        try {
            data = await $.ajax({
                url,
                data: JSON.stringify(createBundleData),
                contentType: 'application/json',
                type: 'POST',
            });
    
        } catch (error) {
            this.clearProgress();
            alert(createAlertText(url, error.responseText));
        }

        const bundleUuid = data.data[0].id;
        url =
            '/rest/bundles/' +
            bundleUuid +
            '/contents/blob/?' +
            getQueryParams(folderName + ".zip");
        try {
            await $.ajax({
                url: url,
                type: 'PUT',
                contentType: 'application/octet-stream',
                data: new Blob([bytesArray]),
                processData: false,
                xhr: () => {
                    let xhr = new window.XMLHttpRequest();
                    xhr.upload.addEventListener(
                        'progress',
                        (evt) => {
                            if (evt.lengthComputable) {
                                const percentComplete = parseInt(
                                    (evt.loaded / evt.total) * 100,
                                );
                                this.setState({ percentComplete });
                            }
                        },
                        false,
                    );
                    return xhr;
                },
            });
            this.clearProgress();
            const moveIndex = true;
            const param = { moveIndex };
            this.props.reloadWorksheet(undefined, undefined, param);
            this.props.onUploadFinish();
        } catch(error) {
            this.clearProgress();
            alert(
                createAlertText(
                    url,
                    error.responseText,
                    'refresh and try again.',
                ),
            );
            this.props.onUploadFinish();
        }
    }

    clearProgress = () => {
        this.setState({ numeratorComplete: 0, denominatorComplete: 0, uploading: false });
    }

    render() {
        const { classes } = this.props;
        const { numeratorComplete, denominatorComplete, uploading } = this.state;
        const progressbarVal = parseInt((numeratorComplete / denominatorComplete) * 100) || 0;

        return (
            <React.Fragment>
                <input
                    key={this.props.after_sort_key} // force the NewUpload to update when the focused item changes

                    type="file"
                    id="codalab-file-upload-input"
                    multiple={true}
                    style={ { display: "none" } }
                    ref={this.inputFile}
                    onChange={this.setFile}
                />
                <input
                    id="codalab-dir-upload-input"
                    type="file"
                    directory="true"
                    webkitdirectory="true"
                    mozdirectory="true"
                    style={ { display: "none" } }
                    ref={this.inputFolder}
                    onChange={this.setFolder}
                />
                { uploading && <CircularProgressbar
                        className={ classes.progress }
                        variant="determinate"
                        value={ progressbarVal }
                        text={`${progressbarVal }% uploaded`}
                        styles={buildStyles({
                            textSize: '12px',
                        })}
                    />
                }
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
