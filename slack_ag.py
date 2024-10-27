import os
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import llm

slack_token = os.environ["SLACK_BOT_TOKEN"]
rob_bot_token = os.environ["SLACK_ROB_BOT_TOKEN"]
client = WebClient(token=slack_token)
rob_bot_client = WebClient(token=rob_bot_token)


def get_user_name(user_id):
    try:
        result = client.users_info(user=user_id)
        return result["user"]["profile"]["first_name"]
    except SlackApiError as e:
        print(f"Error fetching user info: {e}")
        return "Unknown"


def fetch_messages(channel_id, oldest_timestamp):
    threads = []
    try:
        result = client.conversations_history(
            channel=channel_id, oldest=oldest_timestamp
        )
        for msg in result["messages"]:
            thread = {
                "thread_ts": msg.get("thread_ts", msg["ts"]),
                "messages": [
                    {
                        "text": msg["text"],
                        "user": get_user_name(msg["user"]),
                        "ts": msg["ts"],
                    }
                ],
            }

            if msg.get("reply_count", 0) > 0:
                thread_messages = client.conversations_replies(
                    channel=channel_id, ts=msg["ts"]
                )
                for reply in thread_messages["messages"][
                    1:
                ]:  # Exclude the parent message
                    thread["messages"].append(
                        {
                            "text": reply["text"],
                            "user": get_user_name(reply["user"]),
                            "ts": reply["ts"],
                        }
                    )

            threads.append(thread)

        print(f"Fetched {len(threads)} threads from {channel_id}")
    except SlackApiError as e:
        print(f"Error fetching messages: {e}")
    return threads


def process_messages(threads):
    flattened_messages = []
    for thread in threads:
        thread_ts = thread["thread_ts"]
        for msg in thread["messages"]:
            flattened_messages.append(
                {
                    "thread_ts": thread_ts,
                    "text": msg["text"],
                    "user": msg["user"],
                    "ts": msg["ts"],
                    "is_thread_reply": msg["ts"] != thread_ts,
                }
            )

    df = pd.DataFrame(flattened_messages)
    print(f"Processed {len(df)} messages into DataFrame")
    print(df.head())
    print("Sample:", df.sample(5))
    return df


def get_channel_id(channel_name):
    try:
        channel_name = channel_name.lstrip("#")
        result = client.conversations_list(types="public_channel")
        for channel in result["channels"]:
            if channel["name"] == channel_name:
                return channel["id"]
        print(f"Channel {channel_name} not found")
        return None
    except SlackApiError as e:
        print(f"Error getting channel ID: {e}")
        return None


def format_all_messages(df):
    """
    Readable version of all threads.
    """
    formatted_output = ""
    for _, row in df.iterrows():
        if row["is_thread_reply"]:
            formatted_output += f"  - {row['user']}: {row['text']}\n"
        else:
            formatted_output += f"- {row['user']}: {row['text']}\n"
    return formatted_output


def join_channel(channel_id):
    try:
        result = rob_bot_client.conversations_join(channel=channel_id)
        if result["ok"]:
            print(f"Successfully joined {channel_id}")
            return True
        else:
            print(f"Failed to join {channel_id}: {result['error']}")
            return False
    except SlackApiError as e:
        print(f"Error joining channel: {e}")
        return False


def post_message(message, channel_name="general"):
    try:
        cid = get_channel_id(channel_name)

        if not cid:
            print("Couldn't find #general channel")
            return False

        if not BOT_USER:
            print("Couldn't find bot user")
            return False

        join_channel(channel_id=cid)
        response = rob_bot_client.chat_postMessage(
            channel=cid,
            text=message,
        )

        if response["ok"]:
            print(f"Message posted successfully to #general")
            return True
        else:
            print(f"Failed to post message: {response['error']}")
            return False

    except SlackApiError as e:
        print(f"Error posting message: {e}")
        return False


cached_bots = {
    "rob-bot": {
        "id": "U07UCB1BZ2L",
        "team_id": "T05V68Q42H0",
        "name": "robbot",
        "deleted": False,
        "color": "43761b",
        "real_name": "rob-bot",
        "tz": "America/Los_Angeles",
        "tz_label": "Pacific Daylight Time",
        "tz_offset": -25200,
        "profile": {
            "title": "",
            "phone": "",
            "skype": "",
            "real_name": "rob-bot",
            "real_name_normalized": "rob-bot",
            "display_name": "",
            "display_name_normalized": "",
            "fields": None,
            "status_text": "",
            "status_emoji": "",
            "status_emoji_display_info": [],
            "status_expiration": 0,
            "avatar_hash": "gce1e5b0cb88",
            "api_app_id": "A07TGU1BWP8",
            "always_active": False,
            "bot_id": "B07TLM8BWS1",
            "first_name": "rob-bot",
            "last_name": "",
            "image_24": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=24&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-24.png",
            "image_32": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=32&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-32.png",
            "image_48": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=48&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-48.png",
            "image_72": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=72&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-72.png",
            "image_192": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=192&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-192.png",
            "image_512": "https://secure.gravatar.com/avatar/ce1e5b0cb8818dd2f17b022f6ba1776d.jpg?s=512&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0009-512.png",
            "status_text_canonical": "",
            "team": "T05V68Q42H0",
        },
        "is_admin": False,
        "is_owner": False,
        "is_primary_owner": False,
        "is_restricted": False,
        "is_ultra_restricted": False,
        "is_bot": True,
        "is_app_user": False,
        "updated": 1730042320,
        "is_email_confirmed": False,
        "who_can_share_contact_card": "EVERYONE",
    }
}


def find_bot_by_name(bot_name):
    if bot_name in cached_bots:
        return cached_bots[bot_name]
    try:
        response = client.users_list()
        for user in response["members"]:
            if user.get("is_bot", False) and user.get("real_name") == bot_name:
                return user
        print(f"Could not find bot with name: {bot_name}")
        return None
    except SlackApiError as e:
        print(f"Error finding bot: {e}")
        return None


BOT_USER = find_bot_by_name("rob-bot")


def main():
    # channels = ["ideating", "general", "random"]
    channels = ["ideating"]
    one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())

    dfs = {}
    for channel in channels:
        channel_id = get_channel_id(channel)
        if channel_id:
            threads = fetch_messages(channel_id, one_week_ago)
            print(f"Fetched {len(threads)} threads from {channel}")
            processed_df = process_messages(threads)
            dfs[channel] = processed_df
            print(f"Processed {len(processed_df)} messages from {channel}")
        else:
            print(f"Skipping channel {channel}")

    for channel, processed_df in dfs.items():
        readable = format_all_messages(processed_df)
        m = llm.get_model("claude-3.5-sonnet")
        response = m.prompt(
            prompt=f"{readable}\n\nWe are a small startup that is ideating on new directions to explore, trying to figure out what to build. For the above set of slack messages that happened this week, extract out the ideas that we discussed, and create a short distillation of the key points and ideas that happened over the entire week.",
            system="You are an expert product manager and startup advisor. Respond with concise distillations of ideas. Ignore messages that are just random chatter. Write in an objective third person tone by just presenting information, avoid a response that addresses the question or interlocutor directly. Response message should be organized and valid markdown",
        )
        t = response.text()
        print(f"Got distillation for {channel}")
        post_message(
            f"Here's a distillation of this week's ideas from #{channel}:\n{t}",
            channel_name="general",
        )
        print(t)
        # print(channel, "\n\n", readable)


if __name__ == "__main__":
    main()
