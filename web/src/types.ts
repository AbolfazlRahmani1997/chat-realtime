export type Receipt = "sending" | "sent" | "delivered" | "seen";

export type ReactionGroup = {
  emoji: string;
  count: number;
  user_ids?: string[];
};

export type ChatMessage = {
  _id?: string;
  Content?: string;
  Type?: string;
  UniqId?: string;
  RoomID?: string;
  Username?: string;
  ClientID?: string;
  Deliver?: string[];
  Read?: string[];
  Reactions?: ReactionGroup[];
  ForwardFrom?: { from_room_id?: string; username?: string; content?: string };
  Edited?: boolean;
  SecretState?: "locked" | "viewed" | "purged" | "";
  Preview?: string;
  CreatedAt?: string;
  receipt?: Receipt;
};

export type ChatRoom = {
  Id: string;
  Name: string;
  Type?: string;
  Members?: { Id: string; firstname?: string; lastname?: string }[];
  CountMessages?: number;
  Message?: ChatMessage;
};

export type ServerEvent = {
  event_type?: string;
  room_id?: string;
  content?: unknown;
  current_user_id?: string;
  Content?: unknown;
  page?: string;
};

export type ConnectionStatus = "idle" | "connecting" | "open" | "closed";
