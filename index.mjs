import layerUtils from "/opt/nodejs/utils.js";
import knex from "/opt/nodejs/db.js";
import DatabaseTableConstants from "/opt/nodejs/DatabaseTableConstants.js";
import NotificationTypeConstants from "/opt/nodejs/NotificationTypeConstants.js";
import { OAuth2Client } from 'google-auth-library';
import axios from 'axios';
import SqsUtils from "/opt/nodejs/SqsUtils.js";
import LayerConstants from "/opt/nodejs/Constants.js";

let googleOAuth2Client;

const initializeGoogleOAuthClient = async () => {
    console.log("Initializing Google OAuth client");
    const clientId = await layerUtils.getGoogleClientId();
    const clientSecret = await layerUtils.getGoogleClientSecret();
    googleOAuth2Client = new OAuth2Client(clientId, clientSecret, 'postmessage');
};

const getAccessTokenFromRefreshToken = async (refreshToken) => {
    googleOAuth2Client.setCredentials({ refresh_token: refreshToken });

    const { credentials } = await googleOAuth2Client.refreshAccessToken();
    return credentials.access_token;
};

const getGmbReviewsUrl = (accountId, locationId, pageToken) => {
    return pageToken
        ? `https://mybusiness.googleapis.com/v4/accounts/${accountId}/locations/${locationId}/reviews?pageToken=${pageToken}`
        : `https://mybusiness.googleapis.com/v4/accounts/${accountId}/locations/${locationId}/reviews`
}

const getGoogleBusinessLocationUrl = (locationId, readMaskList) => {
    const readMask = readMaskList.join(",");
    return `https://mybusinessbusinessinformation.googleapis.com/v1/locations/${locationId}?readMask=${readMask}`;
}

const getGmbVerificationStatusUrl = (locationId) => {
    return `https://mybusinessverifications.googleapis.com/v1/locations/${locationId}/VoiceOfMerchantState`
}

const getUserIdsByOrganizationId = async (organizationId, knex) => {
    if (!organizationId) return [];
  
    const rows = await knex(DatabaseTableConstants.USER_ORGANIZATION_BRIDGE_TABLE)
      .select('user_id')
      .where({ organization_id: organizationId });
  
    return rows.map(row => row.user_id);
};

function stableStringify(obj) {
    if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);

    if (Array.isArray(obj)) {
      return '[' + obj.map(o => stableStringify(o)).join(',') + ']';
    }

    const keys = Object.keys(obj).sort();

    return '{' + keys
      .map(k => JSON.stringify(k) + ':' + stableStringify(obj[k]))
      .join(',') + '}';
}

const syncLocationDetailsToDatabase = async (gmbLocationId, gmbLocationData, organization_id) => {
    console.log(`🔄 Syncing GMB Location details for ${gmbLocationId}`);

    const newData = {
        id: gmbLocationId,
        business_name: gmbLocationData?.title ?? null,
        business_type: gmbLocationData?.serviceArea?.businessType ?? null,
        language_code: gmbLocationData?.languageCode ?? null,
        region_code: gmbLocationData?.storefrontAddress?.regionCode ?? null,
        postal_code: gmbLocationData?.storefrontAddress?.postalCode ?? null,
        sorting_code: gmbLocationData?.storefrontAddress?.sortingCode ?? null,
        administrative_area: gmbLocationData?.storefrontAddress?.administrativeArea ?? null,
        locality: gmbLocationData?.storefrontAddress?.locality ?? null,
        sublocality: gmbLocationData?.storefrontAddress?.sublocality ?? null,
        address_lines: gmbLocationData?.storefrontAddress?.addressLines ?? null,
        recipients: gmbLocationData?.storefrontAddress?.recipients ?? null,
        service_areas: gmbLocationData?.serviceArea?.places?.placeInfos?.map(p => p.placeName) ?? null,
        verification_status: gmbLocationData?.verificationStatus ?? null,
        place_id: gmbLocationData?.metadata?.placeId ?? null,
        maps_uri: gmbLocationData?.metadata?.mapsUri ?? null,
        website_uri: gmbLocationData?.websiteUri ?? null,
        primary_phone: gmbLocationData?.phoneNumbers?.primaryPhone ?? null,
        primary_category: gmbLocationData?.categories?.primaryCategory?.displayName ?? null,
        additional_categories: gmbLocationData?.categories?.additionalCategories?.map(
            (category) => category.displayName
            ) ?? null,
        description: gmbLocationData?.profile?.description ?? null,
        regular_hours: gmbLocationData?.regularHours ?? null
    };

    const gmbStuffingData = [];

    gmbStuffingData.push({
        gmb_id: gmbLocationId,
        gmb_name: gmbLocationData?.title ?? null,
        description: gmbLocationData?.profile?.description ?? null,
    });

    if (!gmbLocationData.business_name_keyword_stuffed_checked_at) {
        console.log("Sending to SQS to check for keyword stuffing");
        await SqsUtils.batchSendMessages(
            gmbStuffingData,
            LayerConstants.GMB_NAME_KEYWORD_STUFFING_SQS_QUEUE,
        );
    }

    const existing = await knex(DatabaseTableConstants.GMB_LOCATION_TABLE)
        .where({ id: gmbLocationId })
        .first();

    if (!existing) {
        await knex(DatabaseTableConstants.GMB_LOCATION_TABLE).insert(newData);
        console.log(`🆕 Inserted GMB Location: ${gmbLocationId}`);
        // need to fire websocket here to update front end.
    } else {
        let hasChanges = false;

        const changes = [];
        for (const key in newData) {
            const existingValue = existing[key];
            const newValue = newData[key];

            let parsedExisting = existingValue;

            try {
                // If existing value is a stringified object/array, parse it
                parsedExisting = typeof existingValue === 'string' && existingValue.startsWith('{')
                    ? JSON.parse(existingValue)
                    : existingValue;
            } catch (e) {
                // fallback — leave it as-is
            }

            const normExisting = stableStringify(parsedExisting);
            const normNew = stableStringify(newValue);

            if (normExisting !== normNew) {
                console.log(`🔄 GMB Location ${gmbLocationId} has changes for ${key}:`, normExisting, '→', normNew);
                changes.push({ key, existingValue, newValue });
                if (key === "business_name" || key === "description") {
                    console.log("GMB Business Name or Description Changed! Sending to SQS to check for keyword stuffing");
                    await SqsUtils.batchSendMessages(
                        gmbStuffingData,
                        LayerConstants.GMB_NAME_KEYWORD_STUFFING_SQS_QUEUE,
                    );
                }
            }
        }

        if (changes.length > 0) {
            await knex(DatabaseTableConstants.GMB_LOCATION_TABLE)
                .where({ id: gmbLocationId })
                .update(newData);
            console.log(`✏️ Updated GMB Location: ${gmbLocationId}`);
            
            const reviewCount = await knex(DatabaseTableConstants.GMB_REVIEW_TABLE)
                .where({ gmb_id: gmbLocationId })
                .andWhere("is_review_live_on_google", true)
                .count("* as count")
                .first();

            const count = parseInt(reviewCount.count, 10) || 0;

            const prettyNames = {
                business_name:       'Business Name',
                business_type:       'Business Type',
                language_code:       'Language Code',
                region_code:         'Region Code',
                postal_code:         'Postal Code',
                sorting_code:        'Sorting Code',
                administrative_area: 'Administrative Area',
                locality:            'Locality',
                sublocality:         'Sublocality',
                address_lines:       'Address Lines',
                recipients:          'Recipients',
                service_areas:       'Service Areas',
                verification_status: 'Verification Status',
                place_id:            'Place ID',
                website_uri:         'Website URI',
                primary_phone:       'Primary Phone',
                primary_category:    'Primary Category',
                additional_categories: 'Additional Categories',
                description: 'Description',
                regular_hours: 'Regular Hours',
            };
            
                function prettify(key) {
                    if (prettyNames[key]) return prettyNames[key];
                    return key
                        .split('_')
                        .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                        .join(' ');
                }

                const historyInserts = [];
                const notificationsToInsert = [];

                for (const { key, existingValue, newValue } of changes) {
                    const oldData = stableStringify(existingValue);
                    const newData = stableStringify(newValue);

                    if (key === 'verification_status') {
                        const userIds = await getUserIdsByOrganizationId(organization_id, knex);

                        const notificationData = {
                            data: JSON.stringify({
                                gmb_id: gmbLocationId,
                                gmb_name: gmbLocationData?.title,
                                old_status: existingValue,
                                new_status: newValue,
                            }),
                            notification_type_id: await layerUtils.getNotificationTypeId(
                                NotificationTypeConstants.VOICE_OF_MERCHANT_UPDATED,
                                knex,
                            ),
                        };

                        userIds.forEach((userId) => {
                            notificationsToInsert.push({
                                organization_id,
                                user_id: userId,
                                ...notificationData,
                            });
                        });

                        historyInserts.push({
                            gmb_id: gmbLocationId,
                            history_type_id: 1,
                            review_amount: count,
                            data: JSON.stringify({
                                old_status: oldData,
                                new_status: newData,
                            }),
                        });
                    } else {
                        historyInserts.push({
                            gmb_id: gmbLocationId,
                            history_type_id: 2,
                            review_amount: count,
                            data: JSON.stringify({
                                field_that_changed: prettify(key),
                                old_data: oldData,
                                new_data: newData,
                            }),
                        });
                    }
                }
            await knex(DatabaseTableConstants.HISTORY_TABLE).insert(historyInserts);

            if (notificationsToInsert.length > 0) {
                await knex(DatabaseTableConstants.NOTIFICATION_TABLE).insert(notificationsToInsert);
            }
        } else {
            console.log(`✅ GMB Location already up to date: ${gmbLocationId}`);
        }
    }
};

const getGmbVerificationStatus = async (locationId, googleAccessToken) => {
    try {
        const gmbVerificationStatusUrl = getGmbVerificationStatusUrl(locationId);

        const { data } = await axios.get(gmbVerificationStatusUrl, {
            headers: {
                'Authorization': `Bearer ${googleAccessToken}`,
            },
        });

        if (data?.verify?.hasPendingVerification) return 'PENDING';
        if (data?.hasVoiceOfMerchant === false && data?.hasBusinessAuthority === false) return 'HARD_SUSPENDED';
        if (data?.hasVoiceOfMerchant === false && data?.hasBusinessAuthority === true) return 'SOFT_SUSPENDED';
        if (data?.hasVoiceOfMerchant === true && data?.hasBusinessAuthority === true) return 'VERIFIED';

        return 'UNKNOWN';
    } catch (error) {
        throw error;
    }
}

const fetchLocationData = async (locationId, googleAccessToken) => {
    console.log(`📥 Fetching location data for ${locationId}`);

    try {
        const readMaskList = ["name", "languageCode", "storeCode", "title", "storefrontAddress", "serviceArea", "phoneNumbers", "regularHours", "websiteUri", "categories", "metadata", "profile"];
        const googleBusinessLocationsURL = getGoogleBusinessLocationUrl(locationId, readMaskList);

        const locationRes = await axios.get(googleBusinessLocationsURL, {
            headers: {
                'Authorization': `Bearer ${googleAccessToken}`,
            },
        });

        locationRes.data.verificationStatus = await getGmbVerificationStatus(locationId, googleAccessToken);

        return locationRes?.data;
    } catch (error) {
        throw error;
    }
};

const fetchReviews = async (locationId, accountId, googleAccessToken) => {
    console.log(`📥 Fetching reviews for ${locationId}`);

    const allReviews = [];
    let pageToken = null;

    try {
        do {
            const gmbReviewsUrl = getGmbReviewsUrl(accountId, locationId, pageToken);

            const { data } = await axios.get(gmbReviewsUrl, {
                headers: {
                    'Authorization': `Bearer ${googleAccessToken}`,
                },
            });

            if (data?.reviews?.length) {
                allReviews.push(...data.reviews);
            }

            pageToken = data?.nextPageToken || null;

        } while (pageToken);
        return allReviews;
    } catch (error) {
        throw error;
    }
};

const normalize = (val) => {
    if (val === undefined || val === null) return null;
    if (val instanceof Date) return val.toISOString();
    if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(val)) return new Date(val).toISOString();
    return val;
};

const syncReviewsToDatabase = async (gmbId, reviews) => {
    const existingReviews = await knex(DatabaseTableConstants.GMB_REVIEW_TABLE).where({ gmb_id: gmbId });
    const existingReviewMap = Object.fromEntries(existingReviews.map(r => [r.id, r]));

    for (const review of reviews) {
        const newData = {
            id: review.reviewId,
            gmb_id: gmbId,
            reviewerProfilePhotoUrl: review.reviewer?.profilePhotoUrl ?? null,
            reviewerDisplayName: review.reviewer?.displayName ?? null,
            starRating: review.starRating ?? null,
            comment: review.comment ?? null,
            createTime: review.createTime ?? null,
            updateTime: review.updateTime ?? null,
            reviewReplyComment: review.reviewReply?.comment ?? null,
            reviewReplyUpdateTime: review.reviewReply?.updateTime ?? null,
            name: review.name ?? null,
            backed_up_date_time: new Date().toISOString(),
            is_review_live_on_google: true
        };

        const existing = existingReviewMap[review.reviewId];

        if (!existing) {
            await knex(DatabaseTableConstants.GMB_REVIEW_TABLE).insert(newData);
            console.log(`🆕 Inserted new review: ${review.reviewId}`);
            // need to fire websocket here to update front end.
        } else {
            let hasChanges = false;

            const changes = [];
            for (const key in newData) {
                if (normalize(newData[key]) !== normalize(existing[key])) {
                    console.log(`🔄 Review ${review.reviewId} has changes for ${key}: ${existing[key]} -> ${newData[key]}`);
                    changes.push({ key, existingValue: existing[key], newValue: newData[key] });
                }
            }

            if (changes.length > 0) {
                await knex(DatabaseTableConstants.GMB_REVIEW_TABLE).where({ id: review.reviewId }).update(newData);
                console.log(`✏️ Updated review: ${review.reviewId}`);
                // need to fire websocket here to update front end.
            } else {
                console.log(`✅ Review already up to date: ${review.reviewId}`);
            }
        }
    }
};

export const handler = async (event) => {
    const startTime = Date.now();
    console.log("incoming event");
    console.log(event);
    if (!googleOAuth2Client) {
        await initializeGoogleOAuthClient();
    }

    // do this by gmb locations table
    const locations = await knex(DatabaseTableConstants.GMB_LOCATION_ORGANIZATION_BRIDGE_TABLE);

    const allMediaMessages = [];

    for (const location of locations) {
        console.log("Processing location", location);
        const { organization_id, gmb_id, account_id } = location;

        if (!account_id) {
            console.warn(`⚠️ Missing account_id for GMB ${gmb_id}`);
            continue;
        }

        const googleCred = await knex(DatabaseTableConstants.GOOGLE_CREDENTIALS_TABLE)
            .where({ organization_id, sub: account_id })
            .first();

        if (!googleCred) {
            console.warn(`⚠️ No Google credential found for account_id ${account_id}`);
            continue;
        }

        allMediaMessages.push({
            organization_id,
            account_id,
            gmb_id,
        });

        let accessToken;

        // setting access token
        try {
            const decryptedRefreshToken = await layerUtils.decryptGoogleToken(googleCred.google_refresh_token);
            accessToken = await getAccessTokenFromRefreshToken(decryptedRefreshToken);
        } catch (error) {
            console.error(`❌ Error refreshing access token for ${gmb_id}:`, error);
        }

         // try catch to update gmb details
        try {
            const locationData = await fetchLocationData(gmb_id, accessToken);

            await syncLocationDetailsToDatabase(gmb_id, locationData, organization_id);
        } catch (error) {
            console.error(`❌ Error fetching/updating location details for ${gmb_id}:`, error);
        }

        // send to SQS to save reviews
        await SqsUtils.batchSendMessages(
            [{
              organization_id,
              gmb_id,
              account_id,
            }],
            LayerConstants.GMB_REVIEWS_SQS_QUEUE
        );
    }

    await SqsUtils.batchSendMessages(allMediaMessages, LayerConstants.GMB_MEDIA_SQS_QUEUE);

    const endTime = Date.now();
    const executionTime = endTime - startTime;
    console.log(`⏱️ Lambda execution time: ${executionTime}ms`);

    return {
        statusCode: 200,
        body: JSON.stringify('Done pulling reviews and location data for all GMBs 🚀'),
    };
};
