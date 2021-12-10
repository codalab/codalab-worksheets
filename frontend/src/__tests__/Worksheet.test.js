import React from 'react';
import nock from 'nock';
import { render, waitFor, screen } from '../utils/test-utils';
import Worksheet, { getToastMsg } from '../components/worksheets/Worksheet/Worksheet';

describe('render simple worksheet', () => {
    beforeEach(() => {
        nock('http://localhost')
            .get('/rest/user')
            .query(true)
            .reply(200, {
                data: {
                    type: 'users',
                    attributes: {
                        parallel_run_quota: 3,
                        disk_quota: 38654700000,
                        affiliation: null,
                        url: null,
                        email: '',
                        disk_used: 32540100000,
                        date_joined: null,
                        last_login: 'Wed Aug 12 05:02:16 2020',
                        first_name: null,
                        last_name: null,
                        time_used: 56050,
                        time_quota: 3153600000,
                        user_name: 'codalab',
                        notifications: 2,
                    },
                    id: '0',
                },
                meta: { version: '0.5.21' },
            });
    });
    test('with one markdown block', async () => {
        nock('http://localhost')
            .get('/rest/interpret/worksheet/sample_uuid')
            .query(true)
            .reply(200, {
                uuid: '0xb7f80b54717c4752ac03eb41d34f9526',
                name: 'codalab-',
                owner_id: '0',
                title: '',
                frozen: null,
                is_anonymous: false,
                tags: [],
                last_item_id: 3578050,
                permission: 2,
                group_permissions: [
                    {
                        id: 14813,
                        group_uuid: '0xc573c2c89326443a97e96edaf6443e51',
                        group_name: 'public',
                        permission: 1,
                        permission_spec: 'read',
                    },
                ],
                owner_name: 'codalab',
                source: ['', '123', '1132132312'],
                edit_permission: true,
                enable_chat: false,
                permission_spec: 'all',
                blocks: [
                    {
                        sort_keys: [3578045, 3578046],
                        ids: [3578049, 3578050],
                        mode: 'markup_block',
                        text: '123\n1132132312',
                        is_refined: true,
                    },
                ],
                raw_to_block: [
                    [0, 0],
                    [0, 0],
                    [0, 0],
                ],
                block_to_raw: { '0,0': 2 },
                meta: { version: '0.5.21' },
            });

        const comp = render(<Worksheet match={{ params: { uuid: 'sample_uuid' } }} />);
        await waitFor(() => screen.getByText('123 1132132312'));
        expect(nock.isDone());
        expect(comp).toMatchSnapshot();
    });

    test('with one markdown block', async () => {
        nock('http://localhost')
            .get('/rest/interpret/worksheet/sample_uuid')
            .query(true)
            .reply(404, "Not found: '/interpret/worksheet/sample_uuid'");

        const comp = render(<Worksheet match={{ params: { uuid: 'sample_uuid' } }} />);
        await waitFor(() => screen.getByText("Not found: '/worksheets/sample_uuid'"));
        expect(nock.isDone());
        expect(comp).toMatchSnapshot();
    });

    test('getToastMsg', () => {
        expect(getToastMsg('rm', 0, 1)).toEqual('Deleting 1 bundle...');
        expect(getToastMsg('rm', 0, 2)).toEqual('Deleting 2 bundles...');
        expect(getToastMsg('rm', 1, 1)).toEqual('1 bundle deleted!');
        expect(getToastMsg('rm', 1, 2)).toEqual('2 bundles deleted!');
        expect(getToastMsg('random', 0, 2)).toEqual('Executing random command...');
        expect(getToastMsg('random', 1, 2)).toEqual('random command executed!');
    });
});
