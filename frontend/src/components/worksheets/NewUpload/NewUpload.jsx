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

function getQueryParams(filename) {
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
        percentComplete: 0,
    }

    inputFolder = React.createRef();
    inputFile = React.createRef();

    setFile = () => {
        const files = this.inputFile.current.files;

        if (!files.length) {
            return;
        }
        this.uploadFile(files);
    }

    setFolder = () => {
        const files = this.inputFolder.current.files;
        if (!files.length) {
            return;
        }
        this.uploadFolder(files);
    }

    uploadFile = (files) => {
        if (!files) {
            return;
        }
        let folderSize = 0;
        for (const file of files) {
            folderSize += file.size;
        }
        if (folderSize/1024/1024/1024 > 2) {
            alert('File size is large than 2 GB. Please upload your file(s) through CLI.');
            return;
        }
        const { worksheetUUID, after_sort_key } = this.props;
        const { name, description } = this.state;

        for (const file of files) {
            const createBundleData = getDefaultBundleMetadata(name || file.name, description);
            this.setState({
                uploading: true,
            });
            let url = `/rest/bundles?worksheet=${ worksheetUUID }`;
            if (after_sort_key) {
                url += `&after_sort_key=${ after_sort_key }`;
            }
            $.ajax({
                url,
                data: JSON.stringify(createBundleData),
                contentType: 'application/json',
                type: 'POST',
                success: (data, status, jqXHR) => {
                    var bundleUuid = data.data[0].id;
                    var reader = new FileReader();
                    reader.onload = () => {
                        var arrayBuffer = reader.result,
                            bytesArray = new Uint8Array(arrayBuffer);
                        var url =
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
                                var xhr = new window.XMLHttpRequest();
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
                            success: (data, status, jqXHR) => {
                                this.clearProgress();
                                const moveIndex = true;
                                const param = { moveIndex };
                                this.props.reloadWorksheet(undefined, undefined, param);
                                this.props.onUploadFinish();
                            },
                            error: (jqHXR, status, error) => {
                                this.clearProgress();
                                alert(
                                    createAlertText(
                                        reader.url,
                                        jqHXR.responseText,
                                        'refresh and try again.',
                                    ),
                                );
                                this.props.onUploadFinish();
                            },
                        });
                    };
                    reader.readAsArrayBuffer(file);
                },
                error: (jqHXR, status, error) => {
                    this.clearProgress();
                    alert(createAlertText(url, jqHXR.responseText));
                },
            });
        }
    }

    uploadFolder = (files) => {
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
        var folderName = "";
        if (folderNamePos != -1) {
            folderName = files[0].webkitRelativePath.slice(0, folderNamePos)
        }

        const createBundleData = getDefaultBundleMetadata(name || folderName + ".zip", description);
        this.setState({
            uploading: true,
        });
        let url = `/rest/bundles?worksheet=${ worksheetUUID }`;
        if (after_sort_key) {
            url += `&after_sort_key=${ after_sort_key }`;
        }

        var zip = new JSZip();
        [...files].map(file => {
            zip.file(file.webkitRelativePath, file);
        });

        zip.generateAsync({type:"uint8array", compression: "DEFLATE"}).then((bytesArray) => {

            $.ajax({
                url,
                data: JSON.stringify(createBundleData),
                contentType: 'application/json',
                type: 'POST',
                success: (data, status, jqXHR) => {
                    var bundleUuid = data.data[0].id;
                    var url =
                        '/rest/bundles/' +
                        bundleUuid +
                        '/contents/blob/?' +
                        getQueryParams(folderName + ".zip");
                    $.ajax({
                        url: url,
                        type: 'PUT',
                        contentType: 'application/octet-stream',
                        data: new Blob([bytesArray]),
                        processData: false,
                        xhr: () => {
                            var xhr = new window.XMLHttpRequest();
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
                        success: (data, status, jqXHR) => {
                            this.clearProgress();
                            const moveIndex = true;
                            const param = { moveIndex };
                            this.props.reloadWorksheet(undefined, undefined, param);
                            this.props.onUploadFinish();
                        },
                        error: (jqHXR, status, error) => {
                            this.clearProgress();
                            alert(
                                createAlertText(
                                    url,
                                    jqHXR.responseText,
                                    'refresh and try again.',
                                ),
                            );
                            this.props.onUploadFinish();
                        },
                    });
                },
                error: (jqHXR, status, error) => {
                    this.clearProgress();
                    alert(createAlertText(url, jqHXR.responseText));
                },
            });
        });
    }

    clearProgress = () => {
        this.setState({ percentComplete: 0, uploading: false });
    }

    render() {
        const { classes } = this.props;
        const { percentComplete, uploading } = this.state;

        return (

            <React.Fragment>
                <input
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
                        value={ percentComplete }
                        text={`${percentComplete}% uploaded`}
                        styles={buildStyles({
                            textSize: '12px',
                            // pathColor: 'teal',
                            // strokeLinecap: "butt",
                        })}
                    />
                }
            </React.Fragment>
        );
    }
}

const styles = (theme) => ({
    progress: {
        "z-index": 1000,
        position: 'fixed',
        left: '50vw',
        top: '50vh',
        width: 110,
        height: 110,
        transform: 'translateX(-50%) translateY(-50%)',
    },
});

export default withStyles(styles)(NewUpload);
