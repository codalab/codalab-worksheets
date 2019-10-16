// @flow
import React, { useRef } from 'react';
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
            afterItem,
            worksheetUUID,
            reloadWorksheet,
        } = this.props;
        const showInsertButtons = false;
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
                className={classes.container}
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

const ItemWrapperDraggable = (props) => {
    // Sortable example: https://codesandbox.io/s/github/react-dnd/react-dnd/tree/gh-pages/examples_hooks_js/04-sortable/simple?from-embed
    const ref = useRef(null);
    const [{ opacity }, drag] = useDrag({
      item: { type: ItemTypes.ITEM_WRAPPER },
      collect: monitor => ({
        opacity: monitor.isDragging() ? 0.5 : 1,
      }),
    });
    const [{ isOver }, drop] = useDrop({
		accept: ItemTypes.ITEM_WRAPPER,
		drop: () => console.log("dropped", isOver),
		collect: monitor => ({
			isOver: !!monitor.isOver(),
		}),
    });
    drag(drop(ref))
    return (
      <div ref={ref} style={{ opacity }}>
        <ItemWrapperWithStyles {...props} />
      </div>
    )
  }

export default ItemWrapperDraggable;