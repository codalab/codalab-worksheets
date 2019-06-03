// @flow
import * as React from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import Button from '@material-ui/core/Button';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';
import Input from '@material-ui/core/Input';
import UploadIcon from '@material-ui/icons/CloudUpload';
import CircularProgress from '@material-ui/core/CircularProgress';

import {
    createDefaultBundleName,
    pathIsArchive,
    getArchiveExt,
    getDefaultBundleMetadata,
    createAlertText,
    createHandleRedirectFn,
} from '../../../util/worksheet_utils';
import ConfigPanel, {
    ConfigLabel,
    ConfigTextInput,
    ConfigChipInput,
    ConfigCodeInput,
    ConfigSwitchInput,
} from '../ConfigPanel';


// React doesn't support transpilation of directory and
// webkitdirectory properties, have to set them
// programmatically.
class InputDir extends React.Component {
  
  componentDidMount() {
    this.inputDir.directory = true;
    this.inputDir.webkitdirectory = true;
  }

  render() {
    const { eleref, ...others } = this.props;
    return <input
      {...others}
      type="file"
      ref={ (ele) => { this.inputDir = ele; eleref(ele); } }
    />;
  }
}

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

    componentDidMount() {
        this.inputFile.click();
    }

    setFile = (_) => {
        const files = this.inputFile.files;
        if (!files.length) {
            this.props.onClose();
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
                            this.props.reloadWorksheet();
                            this.props.onClose();
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
                    style={ { visibility: 'hidden', position: 'absolute' } }
                    ref={ (ele) => { this.inputFile = ele; } }
                    onChange={ this.setFile }
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

// <div className={classes.spacer}/>
// <ConfigLabel
//     label="Clone from URL"
//     tooltip="Clone an existing bundle on Codalab."
// />
// <ConfigTextInput
//     value={this.state.url}
//     onValueChange={(value) => this.setState({ url: value })}/>

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
