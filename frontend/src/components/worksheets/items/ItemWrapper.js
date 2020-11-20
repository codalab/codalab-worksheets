// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core/styles';
import NewRun from '../NewRun';
import TextEditorItem from './TextEditorItem';
import SchemaItem from './SchemaItem';

class ItemWrapper extends React.Component {
    state = {
        showNewRun: false,
        showNewText: false,
    };

    render() {
        const {
            children,
            classes,
            item,
            after_sort_key,
            worksheetUUID,
            reloadWorksheet,
            id,
        } = this.props;
        const { showNewRun, showNewText, showNewSchema } = this.props;
        if (!item) {
            return null;
        }

        const { isDummyItem } = item;
        return (
            <div
                className={
                    isDummyItem
                        ? ''
                        : item.mode === 'schema_block'
                        ? classes.schemaContainer
                        : classes.container
                }
                id={id}
            >
                {!isDummyItem && <div className={classes.main}>{children}</div>}
                {showNewRun && (
                    <div className={classes.insertBox}>
                        <NewRun
                            after_sort_key={after_sort_key}
                            ws={this.props.ws}
                            onSubmit={() => this.props.onHideNewRun()}
                            reloadWorksheet={reloadWorksheet}
                        />
                    </div>
                )}
                {showNewText && (
                    <TextEditorItem
                        ids={this.props.ids}
                        mode='create'
                        after_sort_key={after_sort_key}
                        worksheetUUID={worksheetUUID}
                        reloadWorksheet={reloadWorksheet}
                        closeEditor={() => {
                            this.props.onHideNewText();
                        }}
                    />
                )}
                {showNewSchema && (
                    <SchemaItem
                        ws={this.props.ws}
                        onSubmit={() => this.props.onHideNewSchema()}
                        reloadWorksheet={reloadWorksheet}
                        editPermission={true}
                        item={{
                            field_rows: [
                                {
                                    field: '',
                                    'generalized-path': '',
                                    'post-processor': null,
                                    from_schema_name: '',
                                },
                            ],
                            header: ['field', 'generalized-path', 'post-processor'],
                            schema_name: '',
                            sort_keys: [after_sort_key + 1],
                        }}
                        create={true}
                        updateSchemaItem={this.props.updateSchemaItem}
                        focusIndex={this.props.focusIndex}
                        subFocusIndex={this.props.subFocusIndex}
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
    schemaContainer: {
        position: 'relative',
        marginBottom: 0,
        zIndex: 5,
    },
    main: {
        zIndex: 10,
        border: `2px solid transparent`,
        '&:hover': {
            backgroundColor: theme.color.grey.lightest,
            border: `2px solid ${theme.color.grey.base}`,
        },
    },
    insertBox: {
        border: `2px solid ${theme.color.primary.base}`,
        margin: '32px 64px !important',
    },
});

export default withStyles(styles)(ItemWrapper);
