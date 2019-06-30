// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleFilled';
import UploadIcon from '@material-ui/icons/CloudUpload';
import TextIcon from '@material-ui/icons/FontDownload';

import NewRun from '../NewRun';
import NewUpload from '../NewUpload';
import TextEditorItem from './TextEditorItem';
import { getMinMaxKeys } from '../../../util/worksheet_utils';

class InsertButtons extends React.Component<{
    classes: {},
    showNewUpload: () => void,
    showNewRun: () => void,
    showNewText: () => void,
}> {
    render() {
        const { classes, showNewUpload, showNewRun, showNewText } = this.props;
        return (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
                className={classes.buttonsPanel}
            >
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add New Upload'
                    onClick={showNewUpload}
                    classes={{ root: classes.buttonRoot }}
                >
                    <UploadIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add New Run'
                    onClick={showNewRun}
                    classes={{ root: classes.buttonRoot }}
                >
                    <RunIcon className={classes.buttonIcon} />
                    Run
                </Button>
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add Text'
                    onClick={showNewText}
                    classes={{ root: classes.buttonRoot }}
                >
                    <TextIcon className={classes.buttonIcon} />
                    Text
                </Button>
            </div>
        );
    }
}

function getIds(item) {
   if (item.mode === 'markup_block') {
    return item.ids;
   } else if (item.mode === 'table_block') {
        if (item.bundles_spec && item.bundles_spec.bundle_infos) {
            return item.bundles_spec.bundle_infos.map((info) => info.id);
        }
   }
   return [];
}

const SENSOR_HEIGHT = 12;

class ItemWrapper extends React.Component {
    state = {
        showNewUpload: 0,
        showNewRun: 0,
        showNewText: 0,
        showInsertButtons: 0,
    };

    showButtons = (ev) => {
        const row = ev.currentTarget;
        const { top, height } = row.getBoundingClientRect();
        const { clientY } = ev;
        const onTop = clientY >= top && clientY <= top + SENSOR_HEIGHT;
        const onBotttom = clientY >= top + height - SENSOR_HEIGHT && clientY <= top + height;
        if (onTop) {
            this.setState({
                showInsertButtons: -1,
            });
        } else if (onBotttom) {
            this.setState({
                showInsertButtons: 1,
            });
        } else {
            this.setState({
                showInsertButtons: 0,
            });
        }
    };

    render() {
        const {
            children,
            classes,
            prevItem,
            item,
            afterItem,
            worksheetUUID,
            reloadWorksheet,
        } = this.props;
        const { showInsertButtons, showNewUpload, showNewRun, showNewText } = this.state;

        if (!item) {
            return null;
        }

        const ids = getIds(item);
        const itemKeys = getMinMaxKeys(item);
        const prevItemKeys = getMinMaxKeys(prevItem);

        let isWorkSheetItem = true;
        if (itemKeys.minKey === null && itemKeys.maxKey === null) {
            // This item isn't really a worksheet item.
            isWorkSheetItem = false;
        }

        return (
            <div
                className={classes.container}
                onMouseMove={this.showButtons}
                onMouseLeave={() => {
                    this.setState({
                        showInsertButtons: 0,
                    });
                }}
            >
                {showInsertButtons === -1 && isWorkSheetItem && (
                    <InsertButtons
                        classes={classes}
                        showNewUpload={() => {
                            this.setState({ showNewUpload: -1 });
                        }}
                        showNewRun={() => {
                            this.setState({ showNewRun: -1 });
                        }}
                        showNewText={() => {
                            this.setState({ showNewText: -1 });
                        }}
                    />
                )}
                {showNewUpload === -1 && (
                    <NewUpload
                        after_sort_key={prevItemKeys.maxKey || itemKeys.minKey - 10}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        onClose={() => this.setState({ showNewUpload: 0 })}
                    />
                )}
                {showNewRun === -1 && (
                    <div className={classes.insertBox}>
                        <NewRun
                            after_sort_key={prevItemKeys.maxKey || itemKeys.minKey - 10}
                            ws={this.props.ws}
                            reloadWorksheet={reloadWorksheet}
                            onSubmit={() => this.setState({ showNewRun: 0 })}
                        />
                    </div>
                )}
                {showNewText === -1 && (
                    <TextEditorItem
                        ids={ids}
                        mode="create"
                        after_sort_key={prevItemKeys.maxKey || itemKeys.minKey - 10}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        closeEditor={() => {
                            this.setState({ showNewText: 0 });
                        }}
                    />
                )}
                <div className={classes.main}>{children}</div>
                {showNewUpload === 1 && (
                    <NewUpload
                        after_sort_key={itemKeys.maxKey}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        onClose={() => this.setState({ showNewUpload: 0 })}
                    />
                )}
                {showNewRun === 1 && (
                    <div className={classes.insertBox}>
                        <NewRun
                            after_sort_key={itemKeys.maxKey}
                            ws={this.props.ws}
                            onSubmit={() => this.setState({ showNewRun: 0 })}
                            reloadWorksheet={reloadWorksheet}
                        />
                    </div>
                )}
                {showNewText === 1 && (
                    <TextEditorItem
                        ids={ids}
                        mode="create"
                        after_sort_key={itemKeys.maxKey}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        closeEditor={() => {
                            this.setState({ showNewText: 0 });
                        }}
                    />
                )}
                {showInsertButtons === 1 && isWorkSheetItem && (
                    <InsertButtons
                        classes={classes}
                        showNewUpload={() => {
                            this.setState({ showNewUpload: 1 });
                        }}
                        showNewRun={() => {
                            this.setState({ showNewRun: 1 });
                        }}
                        showNewText={() => {
                            this.setState({ showNewText: 1 });
                        }}
                    />
                )}
            </div>
        );
    }
}

const styles = (theme) => ({
    container: {
        position: 'relative',
        marginBottom: 20,
        zIndex: 5,
    },
    main: {
        zIndex: 10,
        border: `2px solid transparent`,
        '&:hover': {
            backgroundColor: theme.color.grey.lightest,
            border: `2px solid ${theme.color.grey.base}`,
        }
    },
    buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        overflow: 'visible',
        justifyContent: 'center',
        width: '100%',
        height: 0,
        transform: 'translateY(-16px)',
        zIndex: 20,
    },
    buttonRoot: {
        width: 120,
        height: 32,
        marginLeft: theme.spacing.unit,
        marginRight: theme.spacing.unit,
        backgroundColor: '#f7f7f7',
        '&:hover': {
            backgroundColor: '#f7f7f7',
        },
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    insertBox: {
        border: `2px solid ${theme.color.primary.base}`,
        margin: '32px 64px !important',
    },
});

export default withStyles(styles)(ItemWrapper);
