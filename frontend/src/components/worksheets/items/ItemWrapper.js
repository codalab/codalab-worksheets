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

function getMinMaxKeys(item) {
    if (!item) {
        return { minKey: null, maxKey: null };
    }
    let minKey = null;
    let maxKey = null;
    if (item.mode === 'markup_block') {
        if (item.sort_keys && item.sort_keys.length > 0) {
            const { sort_keys, ids } = item;
            const keys = [];
            sort_keys.forEach((k, idx) => {
                const key = k || ids[idx];
                if (key !== null && key !== undefined) {
                    keys.push(key);
                }
            });
            if (keys.length > 0) {
                minKey = Math.min(...keys);
                maxKey = Math.max(...keys);
            }
        }
    } else if (item.mode === 'table_block') {
        if (item.bundles_spec && item.bundles_spec.bundle_infos) {
            const keys = [];
            item.bundles_spec.bundle_infos.forEach((info) => {
                const key = info.sort_key || info.id;
                if (key !== null && key !== undefined) {
                    keys.push(key);
                }
            });
            if (keys.length > 0) {
                minKey = Math.min(...keys);
                maxKey = Math.max(...keys);
            }
        }
    }
    return { minKey, maxKey };
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

        const itemKeys = getMinMaxKeys(item);
        const prevItemKeys = getMinMaxKeys(prevItem);

        let isWorkSheetItem = true;
        if (itemKeys.minKey === null && itemKeys.maxKey === null) {
            // This item isn't really a worksheet item.
            isWorkSheetItem = false;
        }

        let aroundTextBlock = item.mode === 'markup_block';
        let textBlockId = aroundTextBlock ? item.ids[0] : null;
        let defaultText = aroundTextBlock ? item.text : '';
        let showDefault = 0;
        if (showNewText === -1 && prevItem) {
            // Check if prevItem or item is text block.
            aroundTextBlock = aroundTextBlock || prevItem.mode === 'markup_block';
            if (textBlockId === null && aroundTextBlock) {
                textBlockId = prevItem.ids[0];
                defaultText = prevItem.text;
                // Effectively appending text.
                showDefault = 1;
            }
        }
        if (showNewText === 1 && afterItem) {
            // Check if item or afterItem is text block.
            aroundTextBlock = aroundTextBlock || afterItem.mode === 'markup_block';
            if (textBlockId === null && aroundTextBlock) {
                textBlockId = afterItem.ids[0];
                defaultText = afterItem.text;
                // Effectively prepending text.
                showDefault = -1;
            }
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
                    <NewRun
                        after_sort_key={prevItemKeys.maxKey || itemKeys.minKey - 10}
                        ws={this.props.ws}
                        onSubmit={() => this.setState({ showNewRun: 0 })}
                    />
                )}
                {showNewText === -1 && (
                    <TextEditorItem
                        id={textBlockId}
                        mode={aroundTextBlock ? 'edit' : 'create'}
                        defaultValue={defaultText}
                        showDefault={showDefault || -1}
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
                    <NewRun
                        after_sort_key={itemKeys.maxKey}
                        ws={this.props.ws}
                        onSubmit={() => this.setState({ showNewRun: 0 })}
                    />
                )}
                {showNewText === 1 && (
                    <TextEditorItem
                        id={textBlockId}
                        mode={aroundTextBlock ? 'edit' : 'create'}
                        defaultValue={defaultText}
                        showDefault={showDefault || 1}
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
});

export default withStyles(styles)(ItemWrapper);
