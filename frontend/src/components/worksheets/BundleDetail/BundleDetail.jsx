// @flow
import * as React from 'react';
import { useEffect, useState } from 'react';
import { JsonApiDataStore } from 'jsonapi-datastore';
import { findDOMNode } from 'react-dom';
import useSWR from 'swr';
import { apiWrapper, fetchFileSummary } from '../../../util/apiWrapper';

import ConfigPanel from '../ConfigPanel';
import ErrorMessage from '../ErrorMessage';
import MainContent from './MainContent';
import BundleDetailSideBar from './BundleDetailSideBar';
import BundleActions from './BundleActions';

const BundleDetail = ({
    uuid,
    // Callback on metadata change.
    bundleMetadataChanged,
    contentExpanded,
    onOpen,
    onUpdate,
    rerunItem,
    showNewRerun,
    showDetail,
    handleDetailClick,
    editPermission,
    sidebarExpanded,
    hideBundlePageLink,
    showBorder,
    rowStateInfo,
}) => {
    const [bundleInfo, setBundleInfo] = useState(null);
    const [contentType, setContentType] = useState(null);
    const [fileContents, setFileContents] = useState(null);
    const [stdout, setStdout] = useState(null);
    const [stderr, setStderr] = useState(null);
    const [prevUuid, setPrevUuid] = useState(uuid);
    const [open, setOpen] = useState(true);
    const [fetchingContent, setFetchingContent] = useState(false);
    const [fetchingMetadata, setFetchingMetadata] = useState(false);
    const [contentErrors, setContentErrors] = useState([]);
    const [metadataErrors, setMetadataErrors] = useState([]);
    const [pendingFileSummaryFetches, setPendingFileSummaryFetches] = useState(0);

    useEffect(() => {
        if (uuid !== prevUuid) {
            setPrevUuid(uuid);
            setContentErrors([]);
            setMetadataErrors([]);
        }
    }, [uuid]);

    // If info is not available yet, fetch
    // If bundle is in a state that is possible to transition to a different state, fetch data
    // we have ignored ready|failed|killed states here
    const refreshInterval = bundleInfo?.state?.match(
        'uploading|created|staged|making|starting|preparing|running|finalizing|worker_offline',
    )
        ? 4000
        : 0;

    useEffect(() => {
        const timer = setInterval(() => {
            if (onOpen) {
                onOpen();
            }
        }, 4000);
        return () => clearInterval(timer);
    }, []);

    const fetcherMetadata = (url) => {
        if (!fetchingMetadata) {
            setFetchingMetadata(true);
            return fetch(url, {
                type: 'GET',
                url: url,
                dataType: 'json',
            })
                .then((r) => r.json())
                .catch((error) => {
                    setBundleInfo(null);
                    setContentType(null);
                    setFileContents(null);
                    setStderr(null);
                    setStdout(null);
                    setMetadataErrors((metadataErrors) => metadataErrors.concat([error]));
                })
                .finally(() => {
                    setFetchingMetadata(false);
                });
        }
    };

    const urlMetadata =
        '/rest/bundles/' +
        uuid +
        '?' +
        new URLSearchParams({
            include_display_metadata: 1,
            include: 'owner,group_permissions,host_worksheets',
        }).toString();

    const { dataMetadata, errorMetadata, mutateMetadata } = useSWR(urlMetadata, fetcherMetadata, {
        revalidateOnMount: true,
        refreshInterval: refreshInterval,
        onSuccess: (response, key, config) => {
            // Normalize JSON API doc into simpler object
            const bundleInfo = new JsonApiDataStore().sync(response);
            bundleInfo.editableMetadataFields = response.data.meta.editable_metadata_keys;
            bundleInfo.metadataDescriptions = response.data.meta.metadata_descriptions;
            bundleInfo.metadataType = response.data.meta.metadata_type;
            setBundleInfo(bundleInfo);
            setMetadataErrors([]);
        },
    });

    const fetcherContents = (url) => {
        if (!fetchingContent) {
            setFetchingContent(true);
            return apiWrapper.get(url).catch((error) => {
                // If contents aren't available yet, then also clear stdout and stderr.
                setContentType(null);
                setFileContents(null);
                setStderr(null);
                setStdout(null);
                setContentErrors((contentErrors) => contentErrors.concat([error]));
                setFetchingContent(false);
            });
        }
    };

    const urlContents =
        '/rest/bundles/' + uuid + '/contents/info/' + '?' + new URLSearchParams({ depth: 1 });

    const updateBundleDetail = (response) => {
        const info = response.data;
        if (!info || pendingFileSummaryFetches > 0) return;
        if (info.type === 'file' || info.type === 'link') {
            setPendingFileSummaryFetches((f) => f + 1);
            return fetchFileSummary(uuid, '/')
                .then(function(blob) {
                    setContentType(info.type);
                    setFileContents(blob);
                    setStderr(null);
                    setStdout(null);
                })
                .finally(() => {
                    setPendingFileSummaryFetches((f) => f - 1);
                    setFetchingContent(false);
                });
        } else if (info.type === 'directory') {
            // Get stdout/stderr (important to set things to null).
            let fetchRequests = [];
            let stateUpdate = {
                fileContents: null,
            };

            ['stdout', 'stderr'].forEach(function(name) {
                if (info.contents.some((entry) => entry.name === name)) {
                    setPendingFileSummaryFetches((f) => f + 1);
                    fetchRequests.push(
                        fetchFileSummary(uuid, '/' + name)
                            .then(function(blob) {
                                stateUpdate[name] = blob;
                            })
                            .finally(() => {
                                setPendingFileSummaryFetches((f) => f - 1);
                            }),
                    );
                } else {
                    stateUpdate[name] = null;
                }
            });
            Promise.all(fetchRequests)
                .then((r) => {
                    setContentType(info.type);
                    setFileContents(stateUpdate['fileContents']);
                    if ('stdout' in stateUpdate) {
                        setStdout(stateUpdate['stdout']);
                    }
                    if ('stderr' in stateUpdate) {
                        setStderr(stateUpdate['stderr']);
                    }
                })
                .finally(() => {
                    setFetchingContent(false);
                });
        }
    };
    useSWR(urlContents, fetcherContents, {
        revalidateOnMount: true,
        refreshInterval: refreshInterval,
        onSuccess: (response) => {
            updateBundleDetail(response);
            setContentErrors([]);
        },
    });

    const scrollToNewlyOpenedDetail = (node) => {
        // Only scroll to the bundle detail when it is opened
        if (node && open) {
            findDOMNode(node).scrollIntoView({ block: 'center' });
            // Avoid undesirable scroll
            setOpen(false);
        }
    };

    if (!bundleInfo) {
        if (metadataErrors.length) {
            return <ErrorMessage message='Error: Bundle Unavailable' />;
        }
        return <div></div>;
    }

    if (bundleInfo.bundle_type === 'private') {
        return <ErrorMessage message='Error: Bundle Access Denied' />;
    }

    return (
        <ConfigPanel
            //  The ref is created only once, and that this is the only way to properly create the ref before componentDidMount().
            ref={(node) => scrollToNewlyOpenedDetail(node)}
            buttons={
                <BundleActions
                    showNewRerun={showNewRerun}
                    showDetail={showDetail}
                    handleDetailClick={handleDetailClick}
                    bundleInfo={bundleInfo}
                    rerunItem={rerunItem}
                    onComplete={bundleMetadataChanged}
                    editPermission={editPermission}
                />
            }
            sidebar={
                <BundleDetailSideBar
                    bundleInfo={bundleInfo}
                    rowStateInfo={rowStateInfo}
                    onUpdate={onUpdate}
                    onMetaDataChange={mutateMetadata}
                    expanded={sidebarExpanded}
                    hidePageLink={hideBundlePageLink}
                />
            }
            showBorder={showBorder}
        >
            <MainContent
                bundleInfo={bundleInfo}
                stdout={stdout}
                stderr={stderr}
                fileContents={fileContents}
                contentType={contentType}
                expanded={contentExpanded}
            />
        </ConfigPanel>
    );
};

export default BundleDetail;
