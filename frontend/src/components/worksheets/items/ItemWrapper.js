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
            reloadWorksheet,
            borderTop,
            borderBottom
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

        return (
            <div
                className={classNames(classes.container,
                    borderTop ? classes.borderTop: "",
                    borderBottom ? classes.borderBottom: "")}
            >
                <div className={classes.main}>{children}</div>
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
        marginBottom: 20,
        zIndex: 5,
    },
    borderTop: {
        borderTop: "#1d91c0 solid 2px"
    },
    borderBottom: {
        borderBottom: "#1d91c0 solid 2px"
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
            console.log("dropped", props.item, draggedItemProps.item);
            // await addItems({worksheetUUID: p, })
            // alert(createAlertText(this.url, jqHXR.responseText));
        },
        hover(draggedItemProps, monitor, component) {
            // TODO: Move items out of the way.
        },
        canDrop: (props, monitor) => {
            return true;
        }
    });
    drag(drop(ref))
    return (
      <div ref={ref} style={{ opacity }}>
        <ItemWrapperWithStyles {...props} borderTop={borderTop} borderBottom={borderBottom} />
      </div>
    )
  }

export default ItemWrapperDraggable;