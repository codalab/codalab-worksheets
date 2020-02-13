// @flow
import * as React from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import CircularProgress from '@material-ui/core/CircularProgress';

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

    inputFile = React.createRef();

    setFile = () => {
        const files = this.inputFile.current.files;
        if (!files.length) {
            return;
        }
        this.uploadFile(files[0]);
    }

    uploadFile = (file) => {
        if (!file) {
            return;
        }
        const { worksheetUUID, after_sort_key } = this.props;
        const { name, description } = this.state;
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
                    style={ { visibility: 'hidden', position: 'absolute' } }
                    ref={this.inputFile}
                    onChange={this.setFile}
                />
                { uploading && <CircularProgress
                        className={ classes.progress }
                        variant="determinate"
                        value={ percentComplete }
                        size={ 80 }
                    />
                }
            </React.Fragment>
        );
    }
}

const styles = (theme) => ({
    progress: {
        position: 'fixed',
        left: '50vw',
        top: '50vh',
        width: 80,
        height: 80,
        transform: 'translateX(-50%) translateY(-50%)',
    },
});

export default withStyles(styles)(NewUpload);
