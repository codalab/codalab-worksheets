// @flow
import React, { useRef } from 'react';
import $ from "jquery";
import classNames from 'classnames';
import { withStyles } from '@material-ui/core/styles';
import { useDrag, useDrop } from 'react-dnd';
import NewRun from '../NewRun';
import NewUpload from '../NewUpload';
import TextEditorItem from './TextEditorItem';
import { getMinMaxKeys } from '../../../util/worksheet_utils';

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
        showNewUpload: false,
        showNewRun: false,
        showNewText: false,
    };

    render() {
        const {
            children,
            classes,
            prevItem,
            item,
            worksheetUUID,
            reloadWorksheet
        } = this.props;
        const { showNewUpload, showNewRun, showNewText } = this.props;

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

        const {isDummyItem} = item;

        return (
            <div
                className={isDummyItem ? "": classes.container}
            >
                {!isDummyItem && 
                    <div className={classes.main}>{children}</div>
                }
                {showNewUpload && (
                    <NewUpload
                        after_sort_key={itemKeys.maxKey}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        onClose={() => this.props.onHideNewUpload()}
                    />
                )}
                {showNewRun && (
                    <div className={classes.insertBox}>
                        <NewRun
                            after_sort_key={itemKeys.maxKey}
                            ws={this.props.ws}
                            onSubmit={() => this.props.onHideNewRun()}
                            reloadWorksheet={reloadWorksheet}
                        />
                    </div>
                )}
                {showNewText && (
                    <TextEditorItem
                        ids={ids}
                        mode="create"
                        after_sort_key={itemKeys.maxKey}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        closeEditor={() => {
                            this.props.onHideNewText();
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

const ItemWrapperWithStyles = withStyles(styles)(ItemWrapper);

const ItemTypes = {
    ITEM_WRAPPER: "ItemWrapper"
};

const addItems = async ({worksheetUUID, after_sort_key, ids, items}) => {
    const url = `/rest/worksheets/${worksheetUUID}/add-items`;
    const data = {after_sort_key, ids, items};
    return $.ajax({
        url,
        data: JSON.stringify(data),
        contentType: 'application/json',
        type: 'POST'
    });
}

const ItemWrapperDraggable = (props) => {
    const {worksheetUUID, reloadWorksheet, closeEditor} = props;
    // Sortable example: https://codesandbox.io/s/github/react-dnd/react-dnd/tree/gh-pages/examples_hooks_js/04-sortable/simple?from-embed
    const ref = useRef(null);
    const [{ opacity }, drag] = useDrag({
      item: { type: ItemTypes.ITEM_WRAPPER, item: props.item },
      collect: monitor => ({
        opacity: monitor.isDragging() ? 0.5 : 1,
      }),
      canDrag: () => {
        // Only allow dropping to/from items with defined sort keys.
        const {maxKey} = getMinMaxKeys(props.item);
        return maxKey !== null;
      }
    });
    const [{ borderTop, borderBottom }, drop] = useDrop({
        collect: monitor => {
            const isOver = monitor.isOver();
            const offset = monitor.getSourceClientOffset();
            if (isOver && offset && ref.current) {
                const {top, height} = ref.current.getBoundingClientRect();
                const middle = top + height / 2;
                const mouseY = offset.y;
                return {
                    borderTop: mouseY <= middle,
                    borderBottom: mouseY > middle
                }
            }
            return {
                borderTop: false,
                borderBottom: false
            }
        },
		accept: ItemTypes.ITEM_WRAPPER,
		drop: async (draggedItemProps, monitor, component) => {
            console.log(props, props.item, draggedItemProps.item);
            try {
                const {minKey, maxKey} = getMinMaxKeys(props.item);
                const after_sort_key = maxKey + 1; // borderTop ? minKey - 1 : maxKey + 1;
                if (!after_sort_key) {
                    throw "No sort key to insert found";
                }
                await addItems({
                    worksheetUUID,
                    ids: draggedItemProps.item.ids,
                    items: [...draggedItemProps.item.text.split(/[\n]/), ''],
                    after_sort_key
                });
            }
            catch(e) {
                console.error(e);
                // TODO: Add error handling here.
            }
            reloadWorksheet();
        },
        hover(draggedItemProps, monitor, component) {
            // TODO: Move items out of the way.
        },
        canDrop: (draggedItemProps, monitor) => {
            // Only allow dropping to/from items with defined sort keys.
            const {maxKey} = getMinMaxKeys(draggedItemProps.item);
            return borderBottom && maxKey !== null;
        }
    });
    drag(drop(ref));
    const styles = {
        opacity,
        paddingBottom: 20,
        boxSizing: "border-box",
        ...(borderTop && {
            boxShadow: "0 -2px 0 #1d91c0",
        }),
        ...(borderBottom && {
            boxShadow: "0 2px 0 #1d91c0",
        })
    }
    return (
      <div ref={ref} style={ styles }>
        <ItemWrapperWithStyles {...props} />
      </div>
    )
  }

export default ItemWrapperDraggable;