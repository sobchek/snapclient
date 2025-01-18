/* Ethernet Basic Example

   This example code is in the Public Domain (or CC0 licensed, at your option.)

   Unless required by applicable law or agreed to in writing, this
   software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
   CONDITIONS OF ANY KIND, either express or implied.
*/
#include <stdio.h>
#include <string.h>

#include "driver/gpio.h"
#include "esp_eth.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_netif.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/task.h"
#include "sdkconfig.h"
#if CONFIG_ETH_USE_SPI_ETHERNET
#include "driver/spi_master.h"
#endif  // CONFIG_ETH_USE_SPI_ETHERNET

static const char *TAG = "ETH";

static esp_eth_handle_t s_eth_handle = NULL;
static esp_eth_mac_t *s_mac = NULL;
static esp_eth_phy_t *s_phy = NULL;
static esp_eth_netif_glue_handle_t s_eth_glue = NULL;

/* The event group allows multiple bits for each event, but we only care about
 * two events:
 * - we are connected to the AP with an IP
 * - we failed to connect after the maximum amount of retries */
#define ETH_CONNECTED_BIT BIT0
#define ETH_FAIL_BIT BIT1

static EventGroupHandle_t s_eth_event_group;

/** Event handler for Ethernet events */
static void eth_event_handler(void *arg, esp_event_base_t event_base,
                              int32_t event_id, void *event_data) {
  uint8_t mac_addr[6] = {0};
  /* we can get the Ethernet driver handle from event data */
  esp_eth_handle_t eth_handle = *(esp_eth_handle_t *)event_data;

  switch (event_id) {
    case ETHERNET_EVENT_CONNECTED:
      esp_eth_ioctl(eth_handle, ETH_CMD_G_MAC_ADDR, mac_addr);
      ESP_LOGI(TAG, "Ethernet Link Up");
      ESP_LOGI(TAG, "Ethernet HW Addr %02x:%02x:%02x:%02x:%02x:%02x",
               mac_addr[0], mac_addr[1], mac_addr[2], mac_addr[3], mac_addr[4],
               mac_addr[5]);

      break;
    case ETHERNET_EVENT_DISCONNECTED:
      ESP_LOGI(TAG, "Ethernet Link Down");
      xEventGroupSetBits(s_eth_event_group, ETH_FAIL_BIT);
      break;
    case ETHERNET_EVENT_START:
      ESP_LOGI(TAG, "Ethernet Started");
      break;
    case ETHERNET_EVENT_STOP:
      ESP_LOGI(TAG, "Ethernet Stopped");
      break;
    default:
      break;
  }
}

/** Event handler for IP_EVENT_ETH_GOT_IP */
static void got_ip_event_handler(void *arg, esp_event_base_t event_base,
                                 int32_t event_id, void *event_data) {
  ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
  const esp_netif_ip_info_t *ip_info = &event->ip_info;

  ESP_LOGI(TAG, "Ethernet Got IP Address");
  ESP_LOGI(TAG, "~~~~~~~~~~~");
  ESP_LOGI(TAG, "ETHIP:" IPSTR, IP2STR(&ip_info->ip));
  ESP_LOGI(TAG, "ETHMASK:" IPSTR, IP2STR(&ip_info->netmask));
  ESP_LOGI(TAG, "ETHGW:" IPSTR, IP2STR(&ip_info->gw));
  ESP_LOGI(TAG, "~~~~~~~~~~~");

  xEventGroupSetBits(s_eth_event_group, ETH_CONNECTED_BIT);
}

void eth_init(void) {
  // Initialize TCP/IP network interface (should be called only once in
  // application)
  ESP_ERROR_CHECK(esp_netif_init());
  // Create default event loop that running in background
  ESP_ERROR_CHECK(esp_event_loop_create_default());

  esp_netif_inherent_config_t esp_netif_config =
      ESP_NETIF_INHERENT_DEFAULT_ETH();
  // Warning: the interface desc is used in tests to capture actual connection
  // details (IP, gw, mask)
  esp_netif_config.if_desc = "eth";
  esp_netif_config.route_prio = 64;
  esp_netif_config_t netif_config = {.base = &esp_netif_config,
                                     .stack = ESP_NETIF_NETSTACK_DEFAULT_ETH};
  esp_netif_t *netif = esp_netif_new(&netif_config);
  assert(netif);

  eth_mac_config_t mac_config = ETH_MAC_DEFAULT_CONFIG();
  mac_config.rx_task_stack_size = 2048;
  eth_phy_config_t phy_config = ETH_PHY_DEFAULT_CONFIG();
  phy_config.phy_addr = CONFIG_SNAPCLIENT_ETH_PHY_ADDR;
  phy_config.reset_gpio_num = CONFIG_SNAPCLIENT_ETH_PHY_RST_GPIO;
#if CONFIG_SNAPCLIENT_USE_INTERNAL_ETHERNET
  eth_esp32_emac_config_t esp32_emac_config = ETH_ESP32_EMAC_DEFAULT_CONFIG();
  esp32_emac_config.smi_mdc_gpio_num = CONFIG_SNAPCLIENT_ETH_MDC_GPIO;
  esp32_emac_config.smi_mdio_gpio_num = CONFIG_SNAPCLIENT_ETH_MDIO_GPIO;
  s_mac = esp_eth_mac_new_esp32(&esp32_emac_config, &mac_config);
#if CONFIG_SNAPCLIENT_ETH_PHY_IP101
  s_phy = esp_eth_phy_new_ip101(&phy_config);
#elif CONFIG_SNAPCLIENT_ETH_PHY_RTL8201
  s_phy = esp_eth_phy_new_rtl8201(&phy_config);
#elif CONFIG_SNAPCLIENT_ETH_PHY_LAN8720
  s_phy = esp_eth_phy_new_lan87xx(&phy_config);
#elif CONFIG_SNAPCLIENT_ETH_PHY_DP83848
  s_phy = esp_eth_phy_new_dp83848(&phy_config);
#elif CONFIG_SNAPCLIENT_ETH_PHY_KSZ8041
  s_phy = esp_eth_phy_new_ksz80xx(&phy_config);
#endif
#elif CONFIG_SNAPCLIENT_USE_SPI_ETHERNET
  gpio_install_isr_service(0);
  spi_bus_config_t buscfg = {
      .miso_io_num = CONFIG_SNAPCLIENT_ETH_SPI_MISO_GPIO,
      .mosi_io_num = CONFIG_SNAPCLIENT_ETH_SPI_MOSI_GPIO,
      .sclk_io_num = CONFIG_SNAPCLIENT_ETH_SPI_SCLK_GPIO,
      .quadwp_io_num = -1,
      .quadhd_io_num = -1,
  };
  ESP_ERROR_CHECK(spi_bus_initialize(CONFIG_SNAPCLIENT_ETH_SPI_HOST, &buscfg,
                                     SPI_DMA_CH_AUTO));
  spi_device_interface_config_t spi_devcfg = {
      .mode = 0,
      .clock_speed_hz = CONFIG_SNAPCLIENT_ETH_SPI_CLOCK_MHZ * 1000 * 1000,
      .spics_io_num = CONFIG_SNAPCLIENT_ETH_SPI_CS_GPIO,
      .queue_size = 20};
#if CONFIG_SNAPCLIENT_USE_DM9051
  /* dm9051 ethernet driver is based on spi driver */
  eth_dm9051_config_t dm9051_config =
      ETH_DM9051_DEFAULT_CONFIG(CONFIG_SNAPCLIENT_ETH_SPI_HOST, &spi_devcfg);
  dm9051_config.int_gpio_num = CONFIG_SNAPCLIENT_ETH_SPI_INT_GPIO;
  s_mac = esp_eth_mac_new_dm9051(&dm9051_config, &mac_config);
  s_phy = esp_eth_phy_new_dm9051(&phy_config);
#elif CONFIG_SNAPCLIENT_USE_W5500
  /* w5500 ethernet driver is based on spi driver */
  eth_w5500_config_t w5500_config =
      ETH_W5500_DEFAULT_CONFIG(CONFIG_SNAPCLIENT_ETH_SPI_HOST, &spi_devcfg);
  w5500_config.int_gpio_num = CONFIG_SNAPCLIENT_ETH_SPI_INT_GPIO;
  s_mac = esp_eth_mac_new_w5500(&w5500_config, &mac_config);
  s_phy = esp_eth_phy_new_w5500(&phy_config);
#endif
#elif CONFIG_SNAPCLIENT_USE_OPENETH
  phy_config.autonego_timeout_ms = 100;
  s_mac = esp_eth_mac_new_openeth(&mac_config);
  s_phy = esp_eth_phy_new_dp83848(&phy_config);
#endif

  // Install Ethernet driver
  esp_eth_config_t config = ETH_DEFAULT_CONFIG(s_mac, s_phy);
  ESP_ERROR_CHECK(esp_eth_driver_install(&config, &s_eth_handle));
#if !CONFIG_SNAPCLIENT_USE_INTERNAL_ETHERNET
  /* The SPI Ethernet module might doesn't have a burned factory MAC address, we
     cat to set it manually. We set the ESP_MAC_ETH mac address as the default,
     if you want to use ESP_MAC_EFUSE_CUSTOM mac address, please enable the
     configuration: `ESP_MAC_USE_CUSTOM_MAC_AS_BASE_MAC`
  */
  uint8_t eth_mac[6] = {0};
  ESP_ERROR_CHECK(esp_read_mac(eth_mac, ESP_MAC_ETH));
  ESP_ERROR_CHECK(esp_eth_ioctl(s_eth_handle, ETH_CMD_S_MAC_ADDR, eth_mac));
#endif
  // combine driver with netif
  s_eth_glue = esp_eth_new_netif_glue(s_eth_handle);
  esp_netif_attach(netif, s_eth_glue);

  // Register user defined event handers
  ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID,
                                             &eth_event_handler, NULL));
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_ETH_GOT_IP,
                                             &got_ip_event_handler, NULL));
#ifdef CONFIG_SNAPCLIENT_CONNECT_IPV6
  ESP_ERROR_CHECK(esp_event_handler_register(
      ETH_EVENT, ETHERNET_EVENT_CONNECTED, &on_eth_event, netif));
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_GOT_IP6,
                                             &eth_on_got_ipv6, NULL));
#endif

  esp_eth_start(s_eth_handle);

  /* Waiting until either the connection is established (ETH_CONNECTED_BIT) or
   * connection failed for the maximum number of re-tries (ETH_FAIL_BIT). The
   * bits are set by event_handler() (see above) */
  s_eth_event_group = xEventGroupCreate();
  //    EventBits_t bits =
  xEventGroupWaitBits(s_eth_event_group, ETH_CONNECTED_BIT, pdFALSE, pdFALSE,
                      portMAX_DELAY);
}
