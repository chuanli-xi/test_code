#ifndef BICVOS_PUBLISHER_H_
#define BICVOS_PUBLISHER_H_
#include "transport/transport_def.h"
#include "transport/internal/transport_manager.inl"
#include "transport/internal/meta_data.inl"
#include "transport/publisher_base.h"
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include "tick.h"
#include "process.h"
#include "thread.h"
#include "transport/event_callbacks_internal.h"
#ifndef ANDROID
#include "transport/data_sync_info.h"
#endif
#ifdef __QNX__
#include "synced_time/synced_time.h"
#endif

namespace bicvos
{
  namespace core
  {
    using PublishFinishCallback = std::function<void()>;
    template <typename MessageT>
    class Publisher : public PublisherBase
    {
    public:
      SMART_PTR_DEFINITIONS(Publisher<MessageT>);

      Publisher(const std::string &topic_name, TopicType type,
                const PublisherOptions option, bool callback_mode = true)
          : PublisherBase(topic_name, option, callback_mode)
          , topic_type_(type) ,listener_(this)
      {
        type_name_ = GetTypeName<MessageT>();
        data_type_ = TypeManager::Instance()->GetTypeSupport<MessageT>();
        data_type_.register_type(participant_->GetDDSParticipant().get());

        fdds::TopicQos tqos = fdds::TOPIC_QOS_DEFAULT;
        auto meta_data = MetaDataManager::Instance()->GetTypeMetaData<MessageT>();
        if( (meta_data->GetCategory() == MessageCategory::MSG_CATEGORY_PROTOBUF) ||
          (meta_data->GetCategory() == MessageCategory::MSG_CATEGORY_ROS1_MSG) ||
        (meta_data->GetCategory() == MessageCategory::MSG_CATEGORY_IDL) ) {
          std::vector<eprosima::fastrtps::rtps::octet> user_data;
          auto& defition = meta_data->GetDefition();
          // std::cout << "defition: " << defition <<std::endl;
          user_data.reserve(defition.size() + 1); // 1 for MessageCategory
          user_data.push_back(static_cast<eprosima::fastrtps::rtps::octet>
                              (meta_data->GetCategory()));
          user_data.insert(user_data.end(), defition.begin(), defition.end());
          tqos.topic_data().setValue(user_data);
        }
        auto topic = TransportManager::Instance()->GetDDSTopic(participant_, topic_name_, type_name_, tqos);
        if (!topic)
        {
          throw std::runtime_error(std::string("Create topic error, topic_name:") + topic_name_ + ", type_name: " + type_name_);
        }
        dds_topic_ = topic;

        auto callback_mask = GetCallbackStatusMask_(&options_, callback_mode_);

        auto writer = dds_publisher_->create_datawriter(dds_topic_.get(), options_.qos, &listener_, callback_mask);

        if (!writer)
        {
          throw std::runtime_error(std::string("Create DataWriter error, topic_name:") + topic_name_ + ", type_name: " + type_name_);
        }
        dds_writer_ = std::shared_ptr<fdds::DataWriter>(writer,
          [this](fdds::DataWriter *p){
            this->dds_publisher_->delete_datawriter(p);
          }
        );
        // set event mask for waitset mode,
        if (!callback_mode_)
        {
          auto condition_mask = GetStationConditionMask_(&options_);
          dds_writer_->get_statuscondition().set_enabled_statuses(condition_mask);
        }
      }

      ~Publisher()
      {
      }

      void Publish(const MessageT &msg)
      {
        bicvos::timestamp_t time;
        #if defined(PLATFORM_X86) || defined(ANDROID)
        time = std::chrono::steady_clock::now().time_since_epoch().count();
        #else
        time = bicvos::synced_time::GetSyncedTime();
        #endif
        Publish(msg, time);
      }
      void Publish(const MessageT &msg, bicvos::timestamp_t timestamp) {
        eprosima::fastrtps::Time_t now(static_cast<int32_t>(timestamp / 1000000000), static_cast<uint32_t>(timestamp % 1000000000));
        dds_writer_->write_w_timestamp(const_cast<void*>(static_cast<const void*>(&msg)), fdds::HANDLE_NIL, now);
      }
      #ifndef ANDROID
      void Publish(const MessageT &msg, PublishFinishCallback on_finish) {
        bicvos::timestamp_t time;
        #if defined(PLATFORM_X86)
        time = std::chrono::steady_clock::now().time_since_epoch().count();
        #else
        time = bicvos::synced_time::GetSyncedTime();
        #endif
        Publish(msg, on_finish, time);
      }
      void Publish(const MessageT &msg, PublishFinishCallback on_finish, bicvos::timestamp_t timestamp) {
        eprosima::fastrtps::rtps::WriteParams write_params;
        auto custom_identity = generateCustomSampleIdentity_();
        write_params.related_sample_identity(custom_identity);
        auto sub_counter = GetSubscriberCount();
        std::string msg_name = GetDataSyncInfoName(write_params.related_sample_identity());
        DataSyncInfo *info = MessageManager::Instance()->CreateDataSyncInfoMsg(msg_name);
        ConstructDataSyncInfo(info, write_params.related_sample_identity(), sub_counter, base::GetPid());
        
        eprosima::fastrtps::Time_t now(static_cast<int32_t>(timestamp / 1000000000), static_cast<uint32_t>(timestamp % 1000000000));
        write_params.source_timestamp(now);

        MessageManager::Instance()->RegisterMessage(info, this);
        std::atomic_thread_fence(std::memory_order_release);
        bool result = dds_writer_->write(const_cast<void*>(static_cast<const void*>(&msg)),write_params);
        if(on_finish && result) {
          std::lock_guard<std::mutex> lock(async_callback_map_mutex_);
          async_callbacks_.emplace(msg_name,on_finish);
        }
      }
      #endif

      virtual void HandleMsgDone(const std::string msg_name) final {
        std::lock_guard<std::mutex> lock(async_callback_map_mutex_);
        auto it = async_callbacks_.find(msg_name);
        if(it !=async_callbacks_.end()) {
          it->second();
          async_callbacks_.erase(it);
        }
        else {
          std::cerr << "msg callback not exist, msg_name: " << msg_name <<std::endl;
        }
      }

      // void Publish(tag_t buf_tag);
      // void Publish(const std::vector<tag_t> &bufs);

      // ??? need update in the future
      // size_t GetIntraProcessSubscriberCount() const;
      // size_t GetPublisherCount() const;

      template<typename CallbackType>
      void SetOnNewEventCallback(CallbackType callback) {
        std::lock_guard<std::mutex> lock(event_m_);
        fdds::StatusMask current_mask = dds_writer_->get_status_mask();
        if (callback) {
            SetCallbackAndUpdateMask_<CallbackType>(std::move(callback), current_mask);
        } else {
            ClearCallbackAndUpdateMask_<CallbackType>(current_mask);
        }
        dds_writer_->set_listener(&listener_, current_mask);
      }

      virtual std::vector<void *> GetStatusCondition()
      {
        fdds::StatusCondition& condition= dds_writer_->get_statuscondition();
        return std::vector<void *>{static_cast<void *>(&condition)};
      }

      virtual void handleEvent(fdds::Entity* entity, fdds::StatusMask& status) final {
        (void)entity;
        if(status.is_active(fdds::StatusMask::publication_matched())) {
          fdds::PublicationMatchedStatus matched_status;
          dds_writer_->get_publication_matched_status(matched_status);
          std::runtime_error("publication_matched");
          {
            std::lock_guard<std::mutex> lock(event_m_);
            if(options_.event_callbacks.matched_callback) {
              MatchedStatus status;
              convertToMatchedStatus(status, matched_status);
              options_.event_callbacks.matched_callback(status);
            }
          }
        }
        if(status.is_active(fdds::StatusMask::offered_deadline_missed())) {
          fdds::OfferedDeadlineMissedStatus status;
          dds_writer_->get_offered_deadline_missed_status(status);
          std::lock_guard<std::mutex> lock(event_m_);
          if(options_.event_callbacks.deadline_callback) {
            QosDeadlineMissedInfo deadline_info;
            convertToBaseStatus(deadline_info, status);
            options_.event_callbacks.deadline_callback(deadline_info);
          }
        }
        if(status.is_active(fdds::StatusMask::offered_incompatible_qos())) {
          fdds::OfferedIncompatibleQosStatus status;
          dds_writer_->get_offered_incompatible_qos_status(status);
          std::lock_guard<std::mutex> lock(event_m_);
          if(options_.event_callbacks.incompatible_qos_callback) {
            QosIncompatibleEventStatusInfo incomp_info;
            convertToQosIncompatibleEventStatusInfo(incomp_info, status);
            options_.event_callbacks.incompatible_qos_callback(incomp_info);
          }
        }
        if(status.is_active(fdds::StatusMask::liveliness_lost())) {
          fdds::LivelinessLostStatus status;
          dds_writer_->get_liveliness_lost_status(status);
          std::lock_guard<std::mutex> lock(event_m_);
          if(options_.event_callbacks.liveliness_callback) {
            QosLivelinessLostInfo lost_info;
            convertToBaseStatus(lost_info, status);
            options_.event_callbacks.liveliness_callback(lost_info);
          }
        }
      }

      size_t GetSubscriberCount() const {
        return listener_.subscriptionCount(); 
      }

      // const QosPolicy GetActualQos() const {} ??? need update in the future
    
    private:
      class PubListener : public eprosima::fastdds::dds::DataWriterListener {
       public:
        PubListener(Publisher *publisher) : publisher_(publisher) {}

        void on_publication_matched(
            eprosima::fastdds::dds::DataWriter * /* writer */,
            const eprosima::fastdds::dds::PublicationMatchedStatus &info) final {
          {
            std::lock_guard<std::mutex> lock(discovery_m_);
            if (info.current_count_change == 1) {
              subscriptions_.insert(eprosima::fastrtps::rtps::iHandle2GUID(info.last_subscription_handle));
            } else if (info.current_count_change == -1) {
              subscriptions_.erase(eprosima::fastrtps::rtps::iHandle2GUID(info.last_subscription_handle));
            }
          }
          {
            std::lock_guard<std::mutex> lock(publisher_->event_m_);
            if(publisher_->options_.event_callbacks.matched_callback) {
              MatchedStatus status;
              convertToMatchedStatus(status, info);
              publisher_->options_.event_callbacks.matched_callback(status);
            }
          }
          
        }
        void on_offered_deadline_missed(
            eprosima::fastdds::dds::DataWriter *writer,
            const eprosima::fastdds::dds::OfferedDeadlineMissedStatus &status) final {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          if(publisher_->options_.event_callbacks.deadline_callback) {
            QosDeadlineMissedInfo deadline_info;
            convertToBaseStatus(deadline_info, status);
            publisher_->options_.event_callbacks.deadline_callback(deadline_info);
          }
        }

        void on_liveliness_lost(
            eprosima::fastdds::dds::DataWriter *writer,
            const eprosima::fastdds::dds::LivelinessLostStatus &status) final {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          if(publisher_->options_.event_callbacks.liveliness_callback) {
            QosLivelinessLostInfo lost_info;
            convertToBaseStatus(lost_info, status);
            publisher_->options_.event_callbacks.liveliness_callback(lost_info);
          }
        }

        void on_offered_incompatible_qos(
            eprosima::fastdds::dds::DataWriter *,
            const eprosima::fastdds::dds::OfferedIncompatibleQosStatus &status) final {
          std::lock_guard<std::mutex> lock(publisher_->event_m_);
          if(publisher_->options_.event_callbacks.incompatible_qos_callback) {
            QosIncompatibleEventStatusInfo incomp_info;
            convertToQosIncompatibleEventStatusInfo(incomp_info, status);
            publisher_->options_.event_callbacks.incompatible_qos_callback(incomp_info);
          }
        }

        size_t subscriptionCount() const {
          std::lock_guard<std::mutex> lock(discovery_m_);
          return subscriptions_.size();
        }

       private:
        mutable std::mutex discovery_m_;
        std::set<eprosima::fastrtps::rtps::GUID_t> subscriptions_;
        Publisher *publisher_;
      };
      friend class PubListener;

      template<typename CallbackType>
      void SetCallbackAndUpdateMask_(CallbackType&& callback, fdds::StatusMask& current_mask) {
        if constexpr (std::is_same_v<CallbackType, PublicationMatchedCallbackType>) {
            options_.event_callbacks.matched_callback = std::forward<CallbackType>(callback);
        } else if constexpr (std::is_same_v<CallbackType, QosOfferedDeadlineCallbackType>) {
            options_.event_callbacks.deadline_callback = std::forward<CallbackType>(callback);
            current_mask |= fdds::StatusMask::offered_deadline_missed();
        } else if constexpr (std::is_same_v<CallbackType, QosOfferedIncompatibleQoSCallbackType>) {
            options_.event_callbacks.incompatible_qos_callback = std::forward<CallbackType>(callback);
            current_mask |= fdds::StatusMask::offered_incompatible_qos();
        } else if constexpr (std::is_same_v<CallbackType, QosLivelinessLostCallbackType>) {
            options_.event_callbacks.liveliness_callback = std::forward<CallbackType>(callback);
            current_mask |= fdds::StatusMask::liveliness_lost();
        } else {
            throw std::invalid_argument("Unsupported callback type");
        }
      }

      template<typename CallbackType>
      void ClearCallbackAndUpdateMask_(fdds::StatusMask& current_mask) {
        if constexpr (std::is_same_v<CallbackType, PublicationMatchedCallbackType>) {
            options_.event_callbacks.matched_callback = PublicationMatchedCallbackType();
        } else if constexpr (std::is_same_v<CallbackType, QosOfferedDeadlineCallbackType>) {
            options_.event_callbacks.deadline_callback = QosOfferedDeadlineCallbackType();
            current_mask &= ~fdds::StatusMask::offered_deadline_missed();
        } else if constexpr (std::is_same_v<CallbackType, QosOfferedIncompatibleQoSCallbackType>) {
            options_.event_callbacks.incompatible_qos_callback = QosOfferedIncompatibleQoSCallbackType();
            current_mask &= ~fdds::StatusMask::offered_incompatible_qos();
        } else if constexpr (std::is_same_v<CallbackType, QosLivelinessLostCallbackType>) {
            options_.event_callbacks.liveliness_callback = QosLivelinessLostCallbackType();
            current_mask &= ~fdds::StatusMask::liveliness_lost();
        } else {
            throw std::invalid_argument("Unsupported callback type");
        }
      }

      eprosima::fastrtps::rtps::SampleIdentity generateCustomSampleIdentity_() {
        eprosima::fastrtps::rtps::SampleIdentity custom_identity;
        custom_identity.writer_guid(dds_writer_->guid());
        uint64_t seq = ++sequence_number_;
        eprosima::fastrtps::rtps::SequenceNumber_t seq_num(seq);
        custom_identity.sequence_number(seq_num);
        return custom_identity;
      }

      std::mutex event_m_;
      std::shared_ptr<fdds::Topic> dds_topic_;
      std::shared_ptr<fdds::DataWriter> dds_writer_;
      std::atomic<uint64_t> sequence_number_{1000};
      std::string type_name_;
      TopicType topic_type_ = TopicType::TOPIC_MESSAGE;
      std::mutex async_callback_map_mutex_;
      std::unordered_map<std::string,PublishFinishCallback> async_callbacks_;
      fdds::TypeSupport data_type_;
      PubListener listener_;
    };
  }
}
#endif