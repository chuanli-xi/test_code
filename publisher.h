#pragma once
#include "transport/transport_def.h"
#include "transport/internal/transport_manager.inl"
#include "transport/internal/meta_data.inl"
#include "transport/internal/type_support.inl"
#include "transport/publisher_base.h"
#include "transport/event_callbacks.h"
#include "transport/serdata.h"
#include "tick.h"
#include "process.h"
#include "thread.h"
#ifdef __QNX__
#include "synced_time/synced_time.h"
#endif

namespace bicvos::core {
  template <typename MessageT>
  class Publisher : public PublisherBase
  {
  public:
    SMART_PTR_DEFINITIONS(Publisher<MessageT>);

    Publisher(const std::string &topic_name, TopicType type,
              const PublisherOptions option, bool callback_mode = true)
        : PublisherBase(topic_name, option, callback_mode)
        , topic_type_(type), listener_(this)
    {
      type_name_ = GetTypeName<MessageT>();
      auto topicQos = dds_create_qos();
      auto meta_data = MetaDataManager::Instance()->GetTypeMetaData<MessageT>();
      auto& defition = meta_data->GetDefition();
      std::cout << "defition: " << defition <<std::endl;
      std::vector<uint8_t> user_data;
      user_data.resize(defition.size() + 1);
      user_data[0] = static_cast<uint8_t>(meta_data->GetCategory());
      memcpy(user_data.data() + 1, defition.data(), defition.size());
      dds_qset_topicdata(topicQos, user_data.data(), user_data.size());
      if constexpr(is_idl_struct<MessageT>::value) {
        dds_topic_ = dds_create_topic(participant_->GetDDSParticipant(), get_topic_descriptor<MessageT>(), topic_name.c_str(), topicQos, NULL);
      }
      else {
        struct ddsi_sertype *ser_type = create_sertype<MessageT>(type_name_, false);
        dds_topic_ = dds_create_topic_sertype(participant_->GetDDSParticipant(), 
                      topic_name.c_str(), &ser_type, topicQos, NULL, NULL);
      }
      if (dds_topic_ < 0) {
        std::runtime_error("Error, create topic failed, topic name: " + topic_name_);
      }
      dds_delete_qos(topicQos);

      wr_listener_ = dds_create_listener(NULL);
      dds_lset_publication_matched_arg(wr_listener_, PubListener::static_on_publication_matched, &listener_, false);
      dds_lset_offered_deadline_missed_arg(wr_listener_, PubListener::static_on_offered_deadline_missed, &listener_, false);
      dds_lset_offered_incompatible_qos_arg(wr_listener_, PubListener::static_on_offered_incompatible_qos, &listener_, false);
      dds_lset_liveliness_lost_arg(wr_listener_, PubListener::static_on_liveliness_lost, &listener_, false);

      status_mask_ =  DDS_PUBLICATION_MATCHED_STATUS | DDS_OFFERED_DEADLINE_MISSED_STATUS 
                    | DDS_OFFERED_INCOMPATIBLE_QOS_STATUS | DDS_LIVELINESS_LOST_STATUS;

      dds_writer_ = dds_create_writer(dds_publisher_, dds_topic_, options_.qos, callback_mode_ ? wr_listener_ : NULL);
      if(dds_writer_ < 0) {
        LogFatal << "Error, create writer failed, topic name: " << topic_name_ << std::endl;
      }
      if(dds_get_instance_handle(dds_writer_, &instance_handle_) < 0) {
        LogFatal << "get instance handle failed" << std::endl;
      }

      dds_guid_t writer_guid;
      auto rc = dds_get_guid(dds_writer_, &writer_guid);
      if(rc != DDS_RETCODE_OK) {
        LogFatal << "Error, get writer guid failed,  " << dds_strretcode(-rc) << std::endl;
      }
      else {
        memcpy(guid_.data(), writer_guid.v, 16);
      }
      rc = dds_set_status_mask(dds_writer_, status_mask_);
      if(rc != DDS_RETCODE_OK) {
        LogFatal << "Error, set status mask failed,  " << dds_strretcode(-rc) << std::endl;
      }
    }

    ~Publisher()
    {
      dds_delete_listener(wr_listener_);
      dds_delete(dds_writer_);
      dds_delete(dds_topic_);
    }

    bool Publish(const MessageT &msg)
    {
      bicvos::timestamp_t time;
      #if defined(PLATFORM_X86) || defined(ANDROID)
      time = std::chrono::steady_clock::now().time_since_epoch().count();
      #else
      time = bicvos::synced_time::GetSyncedTime();
      #endif
      return Publish(msg, time);
    }
    
    bool Publish(const MessageT &msg, bicvos::timestamp_t timestamp) {
      dds_return_t rc;
      rc = dds_write_ts(dds_writer_,&msg, timestamp);
      // if constexpr(std::is_base_of_v<google::protobuf::Message, MessageT>) {
      //   rc = dds_write_ts(dds_writer_,&msg, timestamp);
      //   bicvos_core_DataContainer data_container;
      //   data_container.data_type = bicvos_core_PROTOBUF;
      //   data_container.payload._length = msg.ByteSizeLong();
      //   data_container.payload._buffer = static_cast<uint8_t *>(dds_alloc (data_container.payload._length));
      //   msg.SerializeToArray(data_container.payload._buffer, data_container.payload._length);
      //   data_container.payload._release = true;
      //   rc = dds_write_ts(dds_writer_, &data_container, timestamp);
      //   dds_free(data_container.payload._buffer);
      // }
      // else if constexpr(is_idl_struct<MessageT>::value) {
      //   rc = dds_write_ts(dds_writer_,&msg, timestamp);
      // }
      // else if constexpr(std::is_trivially_copyable_v<MessageT>) {
      //   bicvos_core_DataContainer data_container;
      //   data_container.data_type = bicvos_core_TRIVIALLY_COPYABLE;
      //   data_container.payload._length = sizeof(MessageT);
      //   data_container.payload._buffer = static_cast<uint8_t *>(dds_alloc (data_container.payload._length));
      //   memcpy(data_container.payload._buffer, &msg, data_container.payload._length);
      //   data_container.payload._release = true;
      //   rc = dds_write_ts(dds_writer_, &data_container, timestamp);
      //   dds_free(data_container.payload._buffer);
      // }
      // else if constexpr(ros::message_traits::IsMessage<MessageT>::value) {
      //   bicvos_core_DataContainer data_container;
      //   data_container.data_type = bicvos_core_ROS_MSG;
      //   data_container.payload._length = ros::serialization::serializationLength(msg);
      //   data_container.payload._buffer = static_cast<uint8_t *>(dds_alloc (data_container.payload._length));
      //   ros::serialization::OStream outStream(data_container.payload._buffer, data_container.payload._length);
      //   ros::serialization::serialize(outStream, msg);
      //   data_container.payload._release = true;
      //   rc = dds_write_ts(dds_writer_, &data_container, timestamp);
      //   dds_free(data_container.payload._buffer);
      // }
      // else {
      //   LogError << "type " << typeid(msg).name() << " not supported" << std::endl;
      //   return;
      // }

      if (rc < 0) {
        LogError << "topic " << topic_name_ << " write failed, msg size: " 
                << sizeof(msg) << ", timestamp: " << timestamp << ", error code: " 
                << dds_strretcode(-rc) << std::endl;
        return false;
      }
      return true;
    }

    // #ifndef ANDROID
    // void Publish(const MessageT &msg, PublishFinishCallback on_finish) {
    // }
    // void Publish(const MessageT &msg, PublishFinishCallback on_finish, bicvos::timestamp_t timestamp) {
    // }
    // #endif

    // virtual void HandleMsgDone(const std::string msg_name) final {
    // }


    template<typename CallbackType>
    void SetOnNewEventCallback(CallbackType callback) {
      if (callback) {
        SetCallbackAndUpdateMask_<CallbackType>(std::move(callback));
      }
      else {
        ClearCallbackAndUpdateMask_<CallbackType>(std::move(callback));
      }
    }

    virtual std::vector<dds_entity_t> GetEntityId() {
      return std::vector<dds_entity_t>{dds_writer_};
    }
    uint64_t GetInstanceHandle() {
      return instance_handle_;
    }
    GuidType GetEntityGuid() {
      return guid_;
    }

    virtual void handleEvent(dds_entity_t entity, uint32_t mask) final {
      assert(entity == dds_writer_);
      PublisherEventCallbacks event_callbacks;

      if(mask & DDS_PUBLICATION_MATCHED_STATUS) {
        dds_publication_matched_status_t status;
        dds_get_publication_matched_status(dds_writer_, &status);
        listener_.on_publication_matched(dds_writer_, status, &listener_);
        // uint32_t status_mask;
        // dds_take_status(entity, &status_mask, DDS_PUBLICATION_MATCHED_STATUS);
      }
      
      if(mask & DDS_OFFERED_DEADLINE_MISSED_STATUS) {
        dds_offered_deadline_missed_status_t status;
        dds_get_offered_deadline_missed_status(dds_writer_, &status);
        listener_.on_offered_deadline_missed(dds_writer_, status, &listener_);
        // uint32_t status_mask;
        // dds_take_status(entity, &status_mask, DDS_OFFERED_DEADLINE_MISSED_STATUS);
      }
      if(mask & DDS_OFFERED_INCOMPATIBLE_QOS_STATUS) {
        dds_offered_incompatible_qos_status_t status;
        dds_get_offered_incompatible_qos_status(dds_writer_, &status);
        listener_.on_offered_incompatible_qos(dds_writer_, status, &listener_);
        // uint32_t status_mask;
        // dds_take_status(entity, &status_mask, DDS_OFFERED_INCOMPATIBLE_QOS_STATUS);
      }
      if(mask & DDS_LIVELINESS_LOST_STATUS) {
        dds_liveliness_lost_status_t status;
        dds_get_liveliness_lost_status(dds_writer_, &status);
        listener_.on_liveliness_lost(dds_writer_, status, &listener_);
        // uint32_t status_mask;
        // dds_take_status(entity, &status_mask, DDS_LIVELINESS_LOST_STATUS);
      }
    }

    size_t GetMatchedCount() const {
      return current_count_.load();
    }

    // const QosPolicy GetActualQos() const {} ??? need update in the future
  
  private:
    template<typename CallbackType>
    void SetCallbackAndUpdateMask_(CallbackType&& callback) {
      std::lock_guard<std::mutex> lock(event_m_);
      if constexpr (std::is_same_v<CallbackType, PublicationMatchedCallbackType>) {
        options_.event_callbacks.matched_callback = callback;
      } 
      else if constexpr (std::is_same_v<CallbackType, QosOfferedDeadlineCallbackType>) {
        options_.event_callbacks.deadline_callback = callback;
      } 
      else if constexpr (std::is_same_v<CallbackType, QosOfferedIncompatibleQoSCallbackType>) {
        options_.event_callbacks.incompatible_qos_callback = callback;
      } 
      else if constexpr (std::is_same_v<CallbackType, QosLivelinessLostCallbackType>) {
        options_.event_callbacks.liveliness_callback = callback;
      } else {
          throw std::invalid_argument("Unsupported callback type");
      }
    }

    template<typename CallbackType>
    void ClearCallbackAndUpdateMask_(CallbackType&& callback) {
      std::lock_guard<std::mutex> lock(event_m_);
      if constexpr (std::is_same_v<CallbackType, PublicationMatchedCallbackType>) {
          options_.event_callbacks.matched_callback = PublicationMatchedCallbackType();
      } else if constexpr (std::is_same_v<CallbackType, QosOfferedDeadlineCallbackType>) {
          options_.event_callbacks.deadline_callback = QosOfferedDeadlineCallbackType();
      } 
      else if constexpr (std::is_same_v<CallbackType, QosOfferedIncompatibleQoSCallbackType>) {
          options_.event_callbacks.incompatible_qos_callback = QosOfferedIncompatibleQoSCallbackType();
      } else if constexpr (std::is_same_v<CallbackType, QosLivelinessLostCallbackType>) {
          options_.event_callbacks.liveliness_callback = QosLivelinessLostCallbackType();
      } else {
          throw std::invalid_argument("Unsupported callback type");
      }
    }

    class PubListener {
     public:
      PubListener(Publisher *publisher) : publisher_(publisher) {}
      void on_offered_deadline_missed(dds_entity_t writer, 
                const dds_offered_deadline_missed_status_t status, void* arg) {
        QosOfferedDeadlineCallbackType callback;
        {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          callback = publisher_->options_.event_callbacks.deadline_callback;
        }
        
        if(callback) {
          QosDeadlineMissedStatus missed_status;
          convertToOfferedDeadlineMissedStatus(missed_status, status);
          callback(missed_status);
        }
      }
      void on_offered_incompatible_qos(dds_entity_t writer, const dds_offered_incompatible_qos_status_t status, void* arg) {
        QosOfferedIncompatibleQoSCallbackType callback;
        {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          callback = publisher_->options_.event_callbacks.incompatible_qos_callback;
        }
        
        if(callback) {
          QosIncompatibleQosStatus incomp_status;
          convertToOfferedIncompatibleQosStatus(incomp_status, status);
          callback(incomp_status);
        }
      }
      void on_publication_matched(dds_entity_t writer, const dds_publication_matched_status_t  status, void* arg) {
        publisher_->current_count_.store(status.current_count);
        PublicationMatchedCallbackType callback;
        {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          callback = publisher_->options_.event_callbacks.matched_callback;
        }
        if(callback) {
          MatchedStatus pub_status;
          convertToPublicationMatchedStatus(pub_status, status);
          callback(pub_status);
        }
      }
      void on_liveliness_lost (dds_entity_t writer, const dds_liveliness_lost_status_t status, void* arg) {
        QosLivelinessLostCallbackType callback;
        {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          callback = publisher_->options_.event_callbacks.liveliness_callback;
        }
        if(callback) {
          QosLivelinessLostStatus list_status;
          convertToLivelinessLostStatus(list_status, status);
          callback(list_status);
        }
      }
      
      static void static_on_publication_matched(dds_entity_t writer, dds_publication_matched_status_t status, void* arg) {
        PubListener* self = static_cast<PubListener*>(arg);
        self->on_publication_matched(writer, status, arg);
      }
      static void static_on_offered_deadline_missed(dds_entity_t writer, dds_offered_deadline_missed_status_t status, void* arg) {
        PubListener* self = static_cast<PubListener*>(arg);
        self->on_offered_deadline_missed(writer, status, arg);
      }
      static void static_on_offered_incompatible_qos(dds_entity_t writer, dds_offered_incompatible_qos_status_t status, void* arg) {
        PubListener* self = static_cast<PubListener*>(arg);
        self->on_offered_incompatible_qos(writer, status, arg);
      }
      static void static_on_liveliness_lost(dds_entity_t writer, dds_liveliness_lost_status_t status, void* arg) {
        PubListener* self = static_cast<PubListener*>(arg);
        self->on_liveliness_lost(writer, status, arg);
      }

     private:
      Publisher *publisher_;
    };
    friend class PubListener;
    
    std::mutex event_m_;
    dds_entity_t dds_topic_;
    dds_entity_t dds_writer_;
    uint64_t instance_handle_ = 0;
    std::atomic<uint32_t> current_count_ {0};
    GuidType guid_;
    std::string type_name_;
    TopicType topic_type_ = TopicType::TOPIC_MESSAGE;
    uint32_t status_mask_ = 0;
    dds_listener_t *wr_listener_;
    PubListener listener_;
  };
}
